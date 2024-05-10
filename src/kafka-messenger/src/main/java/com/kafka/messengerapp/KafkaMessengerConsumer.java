package com.kafka.messengerapp;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


public class KafkaMessengerConsumer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public String userId;

    private final ReceiverOptions<String, String> receiverOptions;

    public KafkaMessengerConsumer(String userId) {
        this.userId = userId;
        // Create properties.
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "messenger-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "simple-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        receiverOptions = ReceiverOptions.create(props);
    }

    public Flux<ReceiverRecord<String, String>> consume(String topic) {
        KafkaReceiver<String, String> kafkaFlux = createReceiver(topic);
        return kafkaFlux.receive().filter(rec -> Objects.equals(rec.key(), userId));
    }

    public KafkaReceiver<String, String> createReceiver(String topic) {
        ReceiverOptions<String, String> options = receiverOptions.subscription(Collections.singleton(topic));
        return KafkaReceiver.create(options);
    }
}
