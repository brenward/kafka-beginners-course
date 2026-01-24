package com.bwardweb.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

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
        for(int i=0;i<10;i++){
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        log.info("send data successfully");
                        log.info("Recevied new metadata \n "
                                + "Topic: " + recordMetadata.topic() + "\n"
                                + "Partition: " + recordMetadata.partition() + "\n"
                                + "Offset: " + recordMetadata.offset() + "\n"
                                + "Timestamp: " + recordMetadata.timestamp() + "\n"
                        );
                    }else{
                        log.error("Error while producing", e);
                    }
                }
            });
        }

        log.info("Flush and close the producer");
        producer.flush();
        producer.close();
    }
}
