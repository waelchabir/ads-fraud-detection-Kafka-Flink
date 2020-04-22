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
import frauddetection.services.{FlinkService, KafkaService}
import org.apache.flink.streaming.api.scala._

import scala.util.Try


object FraudDetector {

  val CLICKS_TOPIC ="clicks"
  val DISPLAYS_TOPIC ="displays"

  def main(args: Array[String]): Unit = {

    val env = FlinkService().initFlinkEnv(enableWebGui = true)
    env.getConfig.disableClosureCleaner()

    val clicksKafkaService = KafkaService(topic = CLICKS_TOPIC)
    val displaysKafkaService = KafkaService(topic = DISPLAYS_TOPIC)

    val clicksSource = env.addSource(FlinkService().establishKafkaConnection(clicksKafkaService)).name("Creating Clicks source")
    val displaysSource = env.addSource(FlinkService().establishKafkaConnection(displaysKafkaService)).name("Creating Displays source")

    val clicks: DataStream[Try[Event]] = clicksSource.map(Event(_)).name("Clicks events Mapping")
    val displays: DataStream[Try[Event]] = displaysSource.map(Event(_)).name("Displays events Mapping")

    val clicks_uids = clicks.map("clicks: "+_.get.uid).name("Clicks Extract UID")
    val dislays_uids = displays.map("displays: "+_.get.uid).name("Displays Extract UID")

    clicks_uids.print()
    dislays_uids.print()

    env.execute()
  }
}
