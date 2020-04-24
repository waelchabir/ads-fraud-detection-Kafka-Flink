package frauddetection.jobs

import frauddetection.constants.BrokerConstants
import frauddetection.entities.Event
import frauddetection.services.{FlinkService, KafkaService}
import frauddetection.utils.TimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

case class FirstFilter()

object FirstFilter {
  def build(env: StreamExecutionEnvironment):  Unit = {

    val clicksKafkaService = KafkaService(topic= BrokerConstants.getClicksTopicName())

    val clicksSource = env
      .addSource(FlinkService().establishKafkaConnection(clicksKafkaService))
      .name("Creating Clicks source")

    val clicks: DataStream[Event] = clicksSource
      .map(Event(_))
      .name("Clicks events Mapping")

    val fraudClicks = clicks
      .assignTimestampsAndWatermarks(TimestampExtractor)
      .map{(_,1)}
      .keyBy(d => (d._1.timestamp, d._1.ip))
      .sum(1)
      .filter(_._2 >= 5)
      .name("Fraud clicks detector")

    fraudClicks.print()

//    val xx = clicks.assignTimestampsAndWatermarks(TimestampExtractor)
//      .map { (_, 1) }
//      .keyBy(_._1.ip)
//      .window(TumblingEventTimeWindows.of(Time.seconds(1)))
////      .window(SlidingEventTimeWindows.of(Time.seconds(100), Time.seconds(100)))
////      .timeWindow(Time.seconds(5), Time.seconds(1))
//      .sum(1)
//      .filter{a => (a._2 >= 5)}

//    xx.print()
  }
}
