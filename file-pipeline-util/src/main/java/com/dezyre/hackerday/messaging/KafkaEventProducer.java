/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dezyre.hackerday.messaging;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 *
 * @author m.enudi
 */
public class KafkaEventProducer {

	private final String topic;
	private final KafkaProducer<String, String> kafkaProducer;

	public KafkaEventProducer(String kafkaBrokerUrl, String topic) {
		this.topic = topic;
		Properties properties = new Properties();
		properties.put("bootstrap.servers", kafkaBrokerUrl);
		properties.put("acks", "all");
		properties.put("retries", 0);
		properties.put("batch.size", 16384);
		properties.put("linger.ms", 1);
		properties.put("buffer.memory", 33554432);
		properties.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");

		kafkaProducer = new KafkaProducer<>(properties);
	}

	public void send(Event event) {
		kafkaProducer.send(new ProducerRecord(this.topic, event.toString()));
	}
}
