package com.flink.messenger;

import com.flink.messenger.utils.FlinkHelper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import com.flink.messenger.model.KafkaEvent;
import com.flink.messenger.serdes.KafkaEventDeserializer;
import com.flink.messenger.serdes.KafkaEventSerializer;


public class FlinkCensor {
    public static final String BOOTSTRAP_SERVERS = "kafka0:9094,kafka1:9094,kafka2:9092";
    public static final String SOURCE_TOPIC = "kafka-messenger-app";
    public static final String SINK_TOPIC = "kafka-messenger-censored-texts";

    public static void main(String[] args) throws Exception {
        KafkaSource<KafkaEvent> kafkaSource = KafkaSource.<KafkaEvent>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(SOURCE_TOPIC)
                .setGroupId("flink-messages")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new KafkaEventDeserializer(SOURCE_TOPIC))
                .setProperty("partition.discovery.interval.ms", "3600000")
                .setProperty("commit.offsets.on.checkpoint", "true")
                .build();

        KafkaSink<KafkaEvent> sink = KafkaSink.<KafkaEvent>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(new KafkaEventSerializer(SINK_TOPIC))
                .setTransactionalIdPrefix("flink-messenger")
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), SOURCE_TOPIC)
                .keyBy(x -> x.key)
                .process(new SpamDetector())
                .map(new CensorWordsMapFunction())
                .sinkTo(sink);

        env.execute("Kafka Message Censor");
    }

    public static class CensorWordsMapFunction implements MapFunction<KafkaEvent, KafkaEvent> {
        @Override
        public KafkaEvent map(KafkaEvent event) {
            String[] message = FlinkHelper.splitIntoTwoParts(event.value);
            String censoredMsg = message[1].replaceAll("fuck", "f**k")
                    .replaceAll("damn", "d**n")
                    .replaceAll("cunt", "c**t")
                    .replaceAll("dick", "d**k");
            return new KafkaEvent(event.key, message[0] + " " + censoredMsg, event.timestamp);
        }
    }
}
