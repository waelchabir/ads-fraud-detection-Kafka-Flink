package frauddetection.jobs

import frauddetection.entities.{Event, Transaction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

case class ThirdFilter()

object ThirdFilter {
  /**
   * pattern 3:
   *    After joining the two queues we get the following features
   *    [[impressionId]], [[display_IP]], [[click_IP]] , [[display_ts]] and [[click_ts]]
   *    whenever the [[click_ts]] occurs before (display_ts - tolerance interval) => Mark click as fraudulent
   */
  def build(clicksStream: DataStream[Event],
            displaysStream: DataStream[Event],
            windowSize: Int,
            toleranceInterval: Int): Unit = {
    val joined = joinStreams(clicksStream, displaysStream, windowSize)
    val fraudulentClicks = joined.filter(t => t.click.timestamp < (t.display.timestamp - toleranceInterval))
    fraudulentClicks.print()
  }

  def joinStreams(
                   clicks: DataStream[Event],
                   displays: DataStream[Event],
                   windowSize: Long) : DataStream[Transaction] = {

    clicks.join(displays)
      .where(_.impressionId)
      .equalTo(_.impressionId)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
      .apply { (c, d) => Transaction(c.impressionId, c, d) }
  }
}