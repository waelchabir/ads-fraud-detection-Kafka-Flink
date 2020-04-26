package frauddetection.jobs

import frauddetection.entities.Event
import frauddetection.utils.TimestampExtractor
import org.apache.flink.streaming.api.scala._


case class FirstFilter()

object FirstFilter {
  /**
  * Pattern 1:
  *     work on the [[Clicks]] queue, if more than 5 clicks occur from the same [[IP]] at the same [[timestamp]], then fire a warning flag
  */
  def build(clicksStream: DataStream[Event]):  Unit = {

    val fraudClicks = clicksStream
      .assignTimestampsAndWatermarks(TimestampExtractor)
      .map{(_,1)}
      .keyBy(d => (d._1.timestamp, d._1.ip))
      .sum(1)
      .filter(_._2 >= 5)
      .map(_._1)
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
