package mqtt2kafka

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.eclipse.paho.client.mqttv3.*
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import scala.collection.Seq


class MqttClient(private val serverURI: String, private val clientId: String) {

    private lateinit var mqttClient: MqttAsyncClient
    var persistence = MemoryPersistence()
    val spark: SparkSession = SparkSession
        .builder()
        .master("local[2]")
        .appName("Mqtt data Application").orCreate
    fun connect2Mqtt() {
        mqttClient = MqttAsyncClient(serverURI, clientId, persistence)
        val connOpts = MqttConnectOptions()
        connOpts.isCleanSession = true
        val callback: MqttCallback = object : MqttCallback {
            override fun connectionLost(t: Throwable) {
                println("Connection lost")
                Thread.sleep(1000)
                println("reconnect")
                connect2Mqtt()
            }
            @Throws(Exception::class)
            override fun messageArrived(topic: String, message: MqttMessage) {
                println("topic - " + topic + ": " + String(message.payload))
                val mapper = jacksonObjectMapper()
                val typeRef = object : TypeReference<DataMqtt>() {}
                val dataMqtt = mapper.readValue(message.payload.contentToString(), typeRef)
                val data = Seq<>
                val rdd = spark.sparkContext().parallelize(data)
            }
            override fun deliveryComplete(token: IMqttDeliveryToken) {}
        }
        mqttClient.setCallback(callback)
        mqttClient.connect(connOpts)
        val options = MqttConnectOptions()
        try {
            mqttClient.connect(options)
        } catch (e: MqttException) {
            e.printStackTrace()
        }

    }

}
