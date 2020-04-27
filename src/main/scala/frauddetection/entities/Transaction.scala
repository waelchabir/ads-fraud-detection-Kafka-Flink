package frauddetection.entities

case class Transaction(impressionId: String,
                       click: Event,
                       display: Event)
