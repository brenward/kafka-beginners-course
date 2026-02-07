package com.bwardweb.kafka.wiki;


import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.EventHandler;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Headers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //Set safe producer configs
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        log.info("Create Producer");
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        Headers headers = Headers.of("User-Agent", "Kafka-Wikimedia-Client/1.0 (https://example.com)");
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url)).headers(headers);
        EventSource eventSource = builder.build();

        //Start producer in another thread
        eventSource.start();

        TimeUnit.MINUTES.sleep(10);
    }
}
