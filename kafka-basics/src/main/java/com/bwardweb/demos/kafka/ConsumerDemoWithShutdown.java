package com.bwardweb.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Create Consumer Properties");

        String groupId = "my-java-application";
        String topic = "demo_java";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        log.info("Create Consumer");
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);

        log.info("Getting reference to the main thread to control shutdown");
        final Thread mainThread = Thread.currentThread();

        log.info("Adding shutdown hook");
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Shutdown exiting: calling callout.wakeup");
                kafkaConsumer.wakeup();

                log.info("Joingin main thread to allow execution of code in main thread");
                try {
                    mainThread.join();
                }catch (InterruptedException ex){
                    ex.printStackTrace();
                }

            }
        });



        log.info("subscribe to topic");
        kafkaConsumer.subscribe(Arrays.asList(topic));

        log.info("poll for data");
        try {
            while (true) {
                log.info("Polling");
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + " Value: " + record.value());
                    log.info("Partition: " + record.partition() + " Offset: " + record.offset());
                }
            }
        }catch (WakeupException ex){
            log.info("Consumer is starting to shutdown");
        }catch (Exception ex){
            log.info("Unexpected exception, ex");
        }finally {
            kafkaConsumer.close();
            log.info("Consumer shutdown gracefully");
        }

    }
}
