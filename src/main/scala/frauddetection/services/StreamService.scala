package frauddetection.services

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

case class StreamService(webUiPort: Int = 8082) {

  def initFlinkEnv(enableWebUi: Boolean, numberExecutorNodes: Int =2):StreamExecutionEnvironment = {
    val conf: Configuration = new Configuration()
    if (enableWebUi) {
      conf.setInteger(RestOptions.PORT, webUiPort)
    }
    val env = StreamExecutionEnvironment.createLocalEnvironment(numberExecutorNodes, conf)
    env
  }
}
