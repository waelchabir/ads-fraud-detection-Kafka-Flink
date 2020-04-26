package frauddetection.jobs

import frauddetection.entities.{Event, EventSorter}
import frauddetection.utils.TimestampExtractor
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


case class SecondFilter()

object SecondFilter {
  /**
   * pattern 2:
   *     from the [[clicks]] queue, when we get same [[impressionId]] from two or more [[ip]]
   */
  def build(clicksStream: DataStream[Event]): Unit = {
    val FRAUD_EVENT_FLAG = 0
    val VALID_EVENT_FLAG = 1

    val fraudClicks = clicksStream
        .map(event => EventSorter(event.impressionId, event.ip, FRAUD_EVENT_FLAG))
        .iterate(
          (iteration: DataStream[EventSorter]) => {
            val oldValidEvents = iteration.filter(es => (es.flag == VALID_EVENT_FLAG))
            val newEvents = iteration.filter(es => (es.flag == FRAUD_EVENT_FLAG))
            val sortedEvents = newEvents
              .connect(oldValidEvents)
              .flatMap(new RaiseAlertFlatMap)
            val output = sortedEvents.filter(event => event.flag == FRAUD_EVENT_FLAG)
            val feedback = sortedEvents.filter(event => event.flag == VALID_EVENT_FLAG)
            (feedback, output)
          }
        )
    fraudClicks.print()
  }

  class RaiseAlertFlatMap extends CoFlatMapFunction[EventSorter, EventSorter, EventSorter] {
    var oldValidEvents: List[EventSorter] = _

    // new_events handler (EventSorter.flag = 0)
    override def flatMap1(value: EventSorter, out: Collector[EventSorter]): Unit = {
      var fraudulentEvent = false
      for (ove <- oldValidEvents) {
        if (value.impressionId == ove.impressionId && value.ip != ove.ip) fraudulentEvent = true
      }
      if (fraudulentEvent) out.collect(value)
      else out.collect(EventSorter(value.impressionId, value.ip, 1))
    }

    // old_valid_events handler (EventSorter.flag = 1)
    override def flatMap2(value: EventSorter, out: Collector[EventSorter]): Unit = {
      oldValidEvents = value :: oldValidEvents
      out.collect(value)
    }
  }
}
