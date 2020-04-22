/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package frauddetection

import frauddetection.entities.Event
import frauddetection.services.{KafkaService, StreamService}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema

import scala.collection.JavaConverters._
import scala.util.Try


object FraudDetector {

  def main(args: Array[String]): Unit = {

    val env = StreamService().initFlinkEnv(true)
    env.getConfig.disableClosureCleaner()

    val kafkaConsumerProperties = KafkaService().initKafkaConsumer()

    val kafkaConsumer = new FlinkKafkaConsumer010(
      List(KafkaService().topic).asJava,
      new JSONKeyValueDeserializationSchema(true),
      kafkaConsumerProperties
    )
    val lines = env.addSource(kafkaConsumer)
    val events: DataStream[Try[Event]] = lines.map(Event(_)).name("Events Mapping")
    val uids = events.map(_.get.uid).name("Extract UID")
    uids.print()
    env.execute()
  }
}