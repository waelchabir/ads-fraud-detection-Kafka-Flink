package frauddetection.entity

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode

import scala.util.Try

case class Event(eventType: String, uid: String, timestamp: String, ip: String, impressionId: String)

object Event {
  def apply(obj: ObjectNode): Try[Event] = {
    Try({
      val eventType = obj.get("eventType").asText()
      val uid = obj.get("uid").asText()
      val timestamp = obj.get("timestamp").asText()
      val ip = obj.get("ip").asText()
      val impressionId = obj.get("impressionId").asText()

      Event(eventType, uid, timestamp, ip, impressionId)
    })
  }
}