package frauddetection.services

import java.util.Properties

case class KafkaService(
                         topic: String,
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
