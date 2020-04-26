package frauddetection.services

import frauddetection.entities.Event
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema

import scala.collection.JavaConverters._

case class FlinkService(webUiPort: Int = 8082) {

  def initFlinkEnv(enableWebGui: Boolean =false, numberExecutorNodes: Int =2):StreamExecutionEnvironment = {
    val conf: Configuration = new Configuration()
    if (enableWebGui) {
      conf.setInteger(RestOptions.PORT, webUiPort)
    }
    val env = StreamExecutionEnvironment.createLocalEnvironment(numberExecutorNodes, conf)
    env
  }
}

object FlinkService {
  def addSource(environment: StreamExecutionEnvironment, kafkaService: KafkaService): DataStream[Event] = {
    val source = environment
      .addSource(establishKafkaConnection(kafkaService))
      .map(Event(_))
      .name("Creating Clicks source")
    source
  }

  def establishKafkaConnection(kafkaService: KafkaService): FlinkKafkaConsumer010[ObjectNode] = {
    val kafkaServiceProperties = kafkaService.initKafkaConsumer()
    val clicksKafkaConsumer = new FlinkKafkaConsumer010(
      List(kafkaService.topic).asJava,
      new JSONKeyValueDeserializationSchema(true),
      kafkaServiceProperties
    )
    clicksKafkaConsumer
  }
}
