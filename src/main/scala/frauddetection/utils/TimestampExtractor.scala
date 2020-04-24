package frauddetection.utils

import frauddetection.entities.Event
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

object TimestampExtractor extends AssignerWithPeriodicWatermarks[Event]  {

  override def extractTimestamp(e: Event, prevElementTimestamp: Long) = {
    e.timestamp
//    System.currentTimeMillis
  }
  override def getCurrentWatermark(): Watermark = {
    new Watermark(System.currentTimeMillis - 5000)
  }
}