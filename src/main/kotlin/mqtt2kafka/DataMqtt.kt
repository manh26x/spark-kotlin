package mqtt2kafka

import java.util.Date

data class DataMqtt (
    val time: Date,
    val value: Float,
    val key: String,
    val construction: String
)
