package com.bwardweb.kafka.wiki;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class WikimediaChangeHandler implements EventHandler {
    KafkaProducer<String,String> kafkaProducer;
    String topic;

    public WikimediaChangeHandler(KafkaProducer<String,String> producer, String topic){
        this.kafkaProducer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        //not required
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) {
        //not required
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in stream reading. ", t);
    }
}
