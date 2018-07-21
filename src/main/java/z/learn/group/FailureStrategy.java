package z.learn.group;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

interface FailureStrategy<K, V> {
    void onCommitFailure(KafkaConsumer<K, V> consumer, ConsumerRecords<K, V> records);

    void onConsumeFailure(KafkaConsumer<K, V> consumer, ConsumerRecords<K, V> records, ConsumeCallback<K, V> callback);
}
