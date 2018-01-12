package kafka

import java.util.concurrent._
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{KafkaConsumer, ProducerRecord}

class EventConsumer(val brokers: String, val topic : String) extends java.io.Serializable {

	private val props = new Properties()
	props.put("bootstrap.servers", this.brokers)
	props.put("client.id", "SparKafkaMessageConsumer")
	props.put("enable.auto.commit", "true");
	props.put("auto.commit.interval.ms", "1000");
	props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
	props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

	private val kafkaConsumer = new KafkaConsumer[String, String](props)

	def start : Unit = {
		consumer.subscribe(Collections.singletonList(this.topic))

		Executors.newSingleThreadExecutor.execute(new Runnable {
		  override def run(): Unit = {
		    while (true) {
		      val records = consumer.poll(1000)
		      for (record <- records) {
		        //do something with record
		        
		      }
		    }
		  }
		})
	}
}