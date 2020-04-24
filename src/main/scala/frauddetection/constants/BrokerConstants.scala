package frauddetection.constants

case class BrokerConstants(
                          clicksTopic: String = "clicks",
                          displaysTopic: String = "displays"
                          )

object BrokerConstants {

  def getClicksTopicName() = {BrokerConstants().clicksTopic}
  def getDisplayTopicName() = {BrokerConstants().displaysTopic}

}
