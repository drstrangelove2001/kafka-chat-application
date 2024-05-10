package com.kafka.messengerapp;

import com.kafka.messengerapp.model.Message;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.HashMap;
import java.util.Map;

public class KafkaMessengerProducer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    private static final String TOPIC = "kafka-messenger-app";
    private final KafkaSender<String, String> sender;

    public KafkaMessengerProducer() {
        // configure Kafka
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "messenger-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        SenderOptions<String, String> senderOptions = SenderOptions.create(props);
        sender = KafkaSender.create(senderOptions);
    }

    public Mono<Void> send(Message message) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, message.getToUserId(), message.getFromUserId() + " " + message.getMessageTxt());
        SenderRecord<String, String, Long> senderRecord = SenderRecord.create(producerRecord, System.currentTimeMillis());
        return sender.send(Mono.just(senderRecord)).then();
    }

    public void close() {
        sender.close();
    }
}
