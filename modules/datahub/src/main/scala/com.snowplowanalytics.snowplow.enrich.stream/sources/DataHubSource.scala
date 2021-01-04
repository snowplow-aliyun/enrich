/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.stream
package sources

import cats.Id
import cats.syntax.either._
import com.aliyun.datahub.client.exception._
import com.aliyun.datahub.client.model.BlobRecordData
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig
import com.aliyun.datahub.clientlibrary.consumer.Consumer
import com.aliyun.datahub.clientlibrary.producer.Producer
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.stream.model.{AliyunDataHub, SentryConfig, StreamsConfig}
import com.snowplowanalytics.snowplow.enrich.stream.sinks.{DataHubSink, Sink}
import io.circe.Json

/** DataHubSource companion object with factory method */
object DataHubSource {
  def create(
    config: StreamsConfig,
    sentryConfig: Option[SentryConfig],
    client: Client[Id, Json],
    adapterRegistry: AdapterRegistry,
    enrichmentRegistry: EnrichmentRegistry[Id],
    processor: Processor
  ): Either[String, DataHubSource] =
    for {
      dhConfig <- config.sourceSink match {
                    case c: AliyunDataHub => c.asRight
                    case _ => "Configured source/sink is not aliyun-data-hub".asLeft
                  }
      goodProducer <- DataHubSink.validateAndCreateProducer(dhConfig, config.out.enriched)
      badProducer <- DataHubSink.validateAndCreateProducer(dhConfig, config.out.bad)
      emitPii = utils.emitPii(enrichmentRegistry)
      _ <- utils.validatePii(emitPii, config.out.pii)
      piiProducer <- config.out.pii match {
                       case Some(_) =>
                         DataHubSink
                           .validateAndCreateProducer(dhConfig, config.out.pii.get)
                           .map(Some(_))
                       case None => None.asRight
                     }
    } yield new DataHubSource(
      goodProducer,
      piiProducer,
      badProducer,
      client,
      adapterRegistry,
      enrichmentRegistry,
      processor,
      config,
      dhConfig,
      sentryConfig
    )
}

/** Source to read events from a Aliyun DataHub topic */
class DataHubSource private (
  goodProducer: Producer,
  piiProducer: Option[Producer],
  badProducer: Producer,
  client: Client[Id, Json],
  adapterRegistry: AdapterRegistry,
  enrichmentRegistry: EnrichmentRegistry[Id],
  processor: Processor,
  config: StreamsConfig,
  dhConfig: AliyunDataHub,
  sentryConfig: Option[SentryConfig]
) extends Source(client, adapterRegistry, enrichmentRegistry, processor, config.out.partitionKey, sentryConfig) {

  private def createThreadLocalSink =
    (producer: Producer) =>
      new ThreadLocal[Sink] {
        override def initialValue: Sink = new DataHubSink(producer)
      }

  override val MaxRecordSize: Option[Int] = Option(4000000)
  override val threadLocalGoodSink: ThreadLocal[Sink] = createThreadLocalSink(goodProducer)
  override val threadLocalBadSink: ThreadLocal[Sink] = createThreadLocalSink(badProducer)
  override val threadLocalPiiSink: Option[ThreadLocal[Sink]] = piiProducer.map(createThreadLocalSink)

  /** Never-ending processing loop over source stream. */
  override def run(): Unit = {

    val consumerConfig = new ConsumerConfig(dhConfig.endpoint, dhConfig.accessId, dhConfig.accessKey)
    var consumer = new Consumer(dhConfig.project, config.in.raw, config.appName, consumerConfig)

    log.info(s"Running Aliyun DataHub subscription: ${config.appName}.")
    log.info(s"Processing raw input DataHub topic: ${config.in.raw}")

    val stop = false
    val maxRetry = 3
    try while (!stop)
      try while (true) {
        val record = consumer.read(maxRetry)
        if (record != null)
          record.getRecordData match {
            case d: BlobRecordData => enrichAndStoreEvents(List(d.getData))
          }
      } catch {
        case e: SubscriptionOffsetResetException =>
          try {
            log.info("Subscription offset reset, retrying.", e)
            consumer.close()
            consumer = new Consumer(dhConfig.project, config.in.raw, config.appName, consumerConfig)
          } catch {
            case e: DatahubClientException =>
              log.error("Fail to create consumer", e)
              throw e
          }
        case e @ (_: InvalidParameterException | _: SubscriptionOfflineException | _: SubscriptionSessionInvalidException |
            _: AuthorizationFailureException | _: NoPermissionException) =>
          log.error("Unable to access DataHub", e)
          throw e
        case e: DatahubClientException =>
          log.warn("Fail to read from DataHub, retrying..", e)
          Thread.sleep(1000)
      } catch {
      case t: Throwable => log.error("Error reading from DataHub", t)
    } finally consumer.close()
  }
}
