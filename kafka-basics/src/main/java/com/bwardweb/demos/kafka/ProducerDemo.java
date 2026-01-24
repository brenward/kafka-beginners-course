package com.bwardweb.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Create Producer Properties");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        log.info("Create Producer");
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        log.info("Creating record");
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");

        log.info("send data");
        producer.send(producerRecord);

        log.info("Flush and close the producer");
        producer.flush();
        producer.close();
    }
}
