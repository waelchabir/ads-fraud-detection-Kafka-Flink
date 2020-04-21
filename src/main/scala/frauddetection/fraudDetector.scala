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

import java.util.Properties

import frauddetection.entity.Event
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema

import scala.collection.JavaConverters._
import scala.util.Try


object fraudDetector {

  case class KafkaConfig(
                   topic: String = "displays",
                   servers: String = "localhost:9092",
                   group: String = "Fraud Detection"
                   )

  def main(args: Array[String]): Unit = {

    def initKafkaConsumer() = {
      val props = new Properties()
      props.setProperty("bootstrap.servers", KafkaConfig().servers)
      props.setProperty("group.id", KafkaConfig().group)
      props
    }

    def initFlinkEnv(isWebUiEnabled: Boolean, numberExecutorNodes: Int =2):StreamExecutionEnvironment = {
      val conf: Configuration = new Configuration()
      if (isWebUiEnabled) {
        conf.setInteger(RestOptions.PORT, 8082)
      }
      val env = StreamExecutionEnvironment.createLocalEnvironment(numberExecutorNodes, conf)
      env
    }

    val env = initFlinkEnv(true)
    env.getConfig.disableClosureCleaner()

    val kafkaConsumerProperties = initKafkaConsumer()

    val kafkaConsumer = new FlinkKafkaConsumer010(
      List(KafkaConfig().topic).asJava,
      new JSONKeyValueDeserializationSchema(true),
      kafkaConsumerProperties
    )
    val lines = env.addSource(kafkaConsumer)
    val events: DataStream[Try[Event]] = lines.map(Event(_)).name("Events Mapping")
    val uids = events.map(_.get.uid).name("Extract UID")
    uids.print()
    env.execute()


//    // setup flink web server config
//    val conf: Configuration = new Configuration()
//    conf.setInteger(RestOptions.PORT, 8082)
//
//    val env = StreamExecutionEnvironment.createLocalEnvironment(2, conf)
//
//    // set kafka properties
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", KafkaConfig().servers)
//    properties.setProperty("group.id", KafkaConfig().group)
//
//    val kafkaConsumer = new FlinkKafkaConsumer[ObjectNode](
//      KafkaConfig().topic,
//      new JSONKeyValueDeserializationSchema(false),
//      properties
//    )
//
//    val clicksInputStream = env
//      .addSource(kafkaConsumer)
//      .name("Kafka-stream")
//
//    clicksInputStream.print()
//
//    env.execute("Flink Scala Kafka Consumer")
  }
}
