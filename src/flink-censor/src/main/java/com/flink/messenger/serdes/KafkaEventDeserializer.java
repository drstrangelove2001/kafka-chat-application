package com.flink.messenger.serdes;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.flink.messenger.model.KafkaEvent;

import java.io.IOException;


public class KafkaEventDeserializer implements KafkaRecordDeserializationSchema<KafkaEvent> {
    public static StringDeserializer keyDeserializer = new StringDeserializer();
    public static StringDeserializer valueDeserializer = new StringDeserializer();

    public String topic;

    public KafkaEventDeserializer() {}

    public KafkaEventDeserializer(String topic) { this.topic = topic; }


    @Override
    public TypeInformation<KafkaEvent> getProducedType() {
        return TypeInformation.of(KafkaEvent.class);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KafkaEvent> out) throws IOException {
        String key = keyDeserializer.deserialize(topic, record.key());
        String value = valueDeserializer.deserialize(topic, record.value());
        long timestamp = record.timestamp();
        out.collect(new KafkaEvent(key, value, timestamp));
    }
}
