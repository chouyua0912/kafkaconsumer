package z.learn.group;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

interface FailureStrategy<K, V> {

    void onStartup(KafkaConsumer<K, V> consumer, ConsumeCallback<K, V> callback);

    void onShutdown(KafkaConsumer<K, V> consumer, ConsumeCallback<K, V> callback);

    void onCommitFailure(KafkaConsumer<K, V> consumer, ConsumerRecords<K, V> records);

    void onConsumeFailure(KafkaConsumer<K, V> consumer, ConsumerRecords<K, V> records, ConsumeCallback<K, V> callback);
}
