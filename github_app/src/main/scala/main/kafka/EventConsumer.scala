package main.kafka

import java.util.concurrent._
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

class EventConsumer(val brokers: String, val topic : String) extends java.io.Serializable {

	private val props = new Properties()
	props.put("bootstrap.servers", this.brokers)
	props.put("client.id", "SparKafkaMessageConsumer")
	props.put("group.id", "file-processing-group")
	props.put("enable.auto.commit", "true");
	props.put("auto.commit.interval.ms", "1000");
	props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
	props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

	private val kafkaConsumer = new KafkaConsumer[String, String](props)

	def start(jarFile: String) : Unit = {
		println(s"Subscribing to topic $topic")
		kafkaConsumer.subscribe(Collections.singletonList(this.topic))

		println(s"Listening for message on topic $topic")
		Executors.newSingleThreadExecutor.execute(new Runnable {
		  override def run(): Unit = {
		    while (true) {
		      val records = kafkaConsumer.poll(1000)
		      for (record <- records.asScala) {
		      	try{
					val messagePayload = record.value
					println(s"Message arrived!! $messagePayload")
					main.ApplicationLauncher.launch(jarFile, Event(messagePayload))
		      	}catch{
		      		case e :Throwable => e.printStackTrace
		      	}
		      }
		    }
		  }
		})
	}
}