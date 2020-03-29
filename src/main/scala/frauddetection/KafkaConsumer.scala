package frauddetection

import java.util.Properties

import frauddetection.fraudDetector.Config
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

class KafkaConsumer(config: Config) {
//  val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//  val properties = new Properties();
//  properties.setProperty("bootstrap.servers", config.servers);
//  properties.setProperty("group.id", config.group);
//
//
//  val stream: DataStream[String] = env
//    .addSource(new FlinkKafkaConsumer[String](config.topic,
//      new SimpleStringSchema(), properties))
//
//  stream
//    .map((s: String) => s"This is a string: $s")
//    .print
//
//  env.execute("Flink Scala Kafka Consumer")
}
