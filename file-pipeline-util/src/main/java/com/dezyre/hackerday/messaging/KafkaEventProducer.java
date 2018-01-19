/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dezyre.hackerday.messaging;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 *
 * @author m.enudi
 */
public class KafkaEventProducer {

	private final KafkaProducer<String, String> kafkaProducer;

	public KafkaEventProducer(String kafkaBrokerUrl) {
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
		Future<RecordMetadata> responseFuture = kafkaProducer
				.send(new ProducerRecord<String, String>(event.getCode(), event
						.getFileName(), event.toString()));
		// force delivery
		try {
			responseFuture.get(3, TimeUnit.SECONDS);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

//	public static void main(String[] args) {
//		new KafkaEventProducer("quickstart.cloudera:9092").send(new Event(
//				"file-ingestion-error", "2015-01-01-15",
//				"Error occured trying to upload the fileKey the hdfs"));
//	}

}
