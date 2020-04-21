package frauddetection.entity

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode

import scala.util.Try

case class Event(eventType: String, uid: String, timestamp: Long, ip: String, impressionId: String)

object Event {
  def apply(obj: ObjectNode): Try[Event] = {
    Try({
      val eventType = obj.findValue("eventType").asText()
      val uid = obj.findValue("uid").asText()
      val timestamp = obj.findValue("timestamp").asLong()
      val ip = obj.findValue("ip").asText()
      val impressionId = obj.findValue("impressionId").asText()

      Event(eventType, uid, timestamp, ip, impressionId)
    })
  }
}