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
package com.snowplowanalytics.snowplow
package enrich.stream
package sinks

import cats.syntax.either._
import com.aliyun.datahub.client.model.{BlobRecordData, RecordEntry}
import com.aliyun.datahub.clientlibrary.config.ProducerConfig
import com.aliyun.datahub.clientlibrary.producer.Producer
import model.AliyunDataHub

import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.JavaConverters._
import scala.util._

/** DataHubSink companion object with factory method */
object DataHubSink {
  def validateAndCreateProducer(dhConfig: AliyunDataHub, topicName: String): Either[String, Producer] =
    Either
      .catchNonFatal {
        createProducer(dhConfig, topicName)
      }
      .leftMap(_.toString)

  /**
   * Instantiates a producer on an existing topic with the given configuration options.
   * This can fail if the producer can't be created.
   * @return a Aliyun DataHub producer
   */
  private def createProducer(dhConfig: AliyunDataHub, topicName: String): Producer = {
    val producerConfig =
      if (dhConfig.securityToken.isDefined)
        new ProducerConfig(dhConfig.endpoint, dhConfig.accessId, dhConfig.accessKey, dhConfig.securityToken.get)
      else
        new ProducerConfig(dhConfig.endpoint, dhConfig.accessId, dhConfig.accessKey)
    new Producer(dhConfig.project, topicName, producerConfig)
  }
}

/** Aliyun DataHub Sink for Scala enrichment */
class DataHubSink(dataHubProducer: Producer) extends Sink {

  /**
   * Side-effecting function to store the EnrichedEvent to the given output stream.
   * EnrichedEvent takes the form of a tab-delimited String until such time as
   * https://github.com/snowplow/snowplow/issues/211 is implemented.
   * @param events List of events together with their partition keys
   * @return whether to send the stored events to DataHub
   */
  override def storeEnrichedEvents(events: List[(String, String)]): Boolean = {
    val entries = events.map { kv =>
      val entry = new RecordEntry
      entry.setPartitionKey(kv._2)
      entry.setRecordData(new BlobRecordData(kv._1.getBytes(UTF_8)))
      entry
    }
    if (entries.nonEmpty)
      dataHubProducer.send(entries.asJava, 3)
    true
  }

  override def flush(): Unit = ()

}
