package com.flink.messenger.serdes;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.flink.messenger.model.KafkaEvent;

import javax.annotation.Nullable;

public class KafkaEventSerializer implements KafkaRecordSerializationSchema<KafkaEvent> {
    public static StringSerializer keySerializer = new StringSerializer();
    public static StringSerializer valueSerializer = new StringSerializer();

    public String topic;

    public KafkaEventSerializer() {}

    public KafkaEventSerializer(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(KafkaEvent element, KafkaSinkContext context, Long timestamp) {
        String key = element.key;
        String value = element.value;
        return new ProducerRecord<>(topic, null, element.timestamp, keySerializer.serialize(topic, key), valueSerializer.serialize(topic, value));
    }
}
