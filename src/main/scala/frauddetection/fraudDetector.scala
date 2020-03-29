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

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.{ConfigConstants, Configuration, RestOptions}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object fraudDetector {

  case class Config(
                   topic: String = "clicks",
                   servers: String = "localhost:9092",
                   group: String = "test"
                   )

  def main(args: Array[String]): Unit = {

//    val parser = new OptionParser[Config]("scopt") {
//      opt[String]('t', "topic").action((x, c) => c.copy(topic = x)).text("Topic to listen to")
//      opt[String]('s', "servers").action((x, c) => c.copy(servers = x)).text("Kafka bootstrap servers")
//      opt[String]('g', "group").action((x, c) => c.copy(servers = x)).text("Group id of the Kafka consumer")
//    }
//
//    parser.parse(args, Config()) match {
//      case Some(config) =>
//        new KafkaConsumer(config)
//
//      case None =>
//        println("Bad arguments")
//    }

    println("Hello !")

    // setup flink web server config
    val conf: Configuration = new Configuration();
    conf.setInteger(RestOptions.PORT, 8082);
    val env = StreamExecutionEnvironment.createLocalEnvironment(2, conf)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", Config().servers)
    properties.setProperty("group.id", Config().group)

    env
      .addSource(new FlinkKafkaConsumer[String](Config().topic, new SimpleStringSchema(), properties))
      .print()
    env.execute("Flink Scala Kafka Consumer")



  }
}
