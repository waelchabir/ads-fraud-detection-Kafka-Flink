package frauddetection.jobs

import frauddetection.entities.Event
import frauddetection.utils.TimestampExtractor
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink


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
      .name("1st filter - Fraud detector")

    fraudClicks.writeAsText("output/filter1", FileSystem.WriteMode.OVERWRITE)
  }
}