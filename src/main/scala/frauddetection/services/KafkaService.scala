package frauddetection.services

import java.util.Properties

import frauddetection.fraudDetector.KafkaConfig

case class KafkaService(
                         topic: String = "displays",
                         servers: String = "localhost:9092",
                         group: String = "Fraud Detection"
                       )
{
  def initKafkaConsumer() = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", servers)
    props.setProperty("group.id", group)
    props
  }
}
