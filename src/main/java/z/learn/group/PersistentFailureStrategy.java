package z.learn.group;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class PersistentFailureStrategy implements FailureStrategy<Integer, String> {

    @Override
    public void onStartup(KafkaConsumer<Integer, String> consumer, ConsumeCallback<Integer, String> callback) {

    }

    @Override
    public void onShutdown(KafkaConsumer<Integer, String> consumer, ConsumeCallback<Integer, String> callback) {

    }

    @Override
    public void onCommitFailure(KafkaConsumer<Integer, String> consumer, ConsumerRecords<Integer, String> records) {

    }

    @Override
    public void onConsumeFailure(KafkaConsumer<Integer, String> consumer, ConsumerRecords<Integer, String> records, ConsumeCallback<Integer, String> callback) {

    }
}
