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

import frauddetection.constants.BrokerConstants
import frauddetection.jobs.{FirstFilter, SecondFilter, ThirdFilter}
import frauddetection.services.{FlinkService, KafkaService}
import org.apache.flink.streaming.api.TimeCharacteristic


object FraudDetector {

  def main(args: Array[String]): Unit = {
    // set up the execution environment
    val env = FlinkService().initFlinkEnv(enableWebGui = true)
    env.getConfig.disableClosureCleaner()
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    // add sources to the environment
    val clicksKafkaService = KafkaService(topic= BrokerConstants.getClicksTopicName())
    val displaysKafkaService = KafkaService(topic= BrokerConstants.getDisplayTopicName())
    val clicks = FlinkService.addSource(env, clicksKafkaService)
    val displays = FlinkService.addSource(env, displaysKafkaService)

    // start jobs
    FirstFilter.build(clicks)
    SecondFilter.build(clicks)
    ThirdFilter.build(clicks, displays, 5000, 300)

    // execute the environment
    env.execute()
  }
}
