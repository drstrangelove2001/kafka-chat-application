package com.flink.messenger;

import com.flink.messenger.model.KafkaEvent;
import com.flink.messenger.utils.FlinkHelper;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import reactor.core.publisher.Flux;

public class SpamDetector extends KeyedProcessFunction<String, KafkaEvent, KafkaEvent> {

    private static final String[] SPAM_KEYWORDS = {"jackpot", "spam", "exclusive", "buy", "limited", "giveaway", "sale", "claim", "confidential", "greetings", "dear", "click", "download", "activate"};

    private static final String MIGHT_BE_SPAM_MESSAGE = "(might be spam!)";

    @Override
    public void processElement(KafkaEvent event, KeyedProcessFunction<String, KafkaEvent, KafkaEvent>.Context ctx, Collector<KafkaEvent> out) {
        String[] message = FlinkHelper.splitIntoTwoParts(event.value);
        boolean isMessageSpam = Boolean.TRUE.equals(Flux.fromArray(SPAM_KEYWORDS)
                .any(keyword -> message[1].toLowerCase().contains(keyword))
                .block());
        if (isMessageSpam) {
            out.collect(new KafkaEvent(event.key, event.value + " " + MIGHT_BE_SPAM_MESSAGE, event.timestamp));
        } else {
            out.collect(event);
        }
    }
}
