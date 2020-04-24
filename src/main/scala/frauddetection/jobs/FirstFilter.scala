package frauddetection.jobs

import frauddetection.constants.BrokerConstants
import frauddetection.entities.Event
import frauddetection.services.{FlinkService, KafkaService}
import frauddetection.utils.TimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

case class FirstFilter()

object FirstFilter {
  def build(env: StreamExecutionEnvironment):  Unit = {

        val clicksKafkaService = KafkaService(topic = BrokerConstants.getClicksTopicName())
        val displaysKafkaService = KafkaService(topic = BrokerConstants.getDisplayTopicName())

        val clicksSource = env
          .addSource(FlinkService().establishKafkaConnection(clicksKafkaService))
          .name("Creating Clicks source")

        val displaysSource = env
          .addSource(FlinkService().establishKafkaConnection(displaysKafkaService))
          .name("Creating Displays source")

        val clicks: DataStream[Event] = clicksSource.map(Event(_)).name("Clicks events Mapping")
        val displays: DataStream[Event] = displaysSource.map(Event(_)).name("Displays events Mapping")

        val xx = clicks.assignTimestampsAndWatermarks(TimestampExtractor)
          .map { (_, 1) }
          .keyBy(_._1.ip)
          .window(SlidingEventTimeWindows.of(Time.seconds(100), Time.seconds(5)))
    //      .timeWindow(Time.seconds(5), Time.seconds(1))
          .sum(1)

        xx.print()
  }
}
