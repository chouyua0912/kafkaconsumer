package z.learn.group;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface ConsumeCallback<K, V> {
    String getTopic();

    int getPartition();

    void onMessage(ConsumerRecords<K, V> records);

    ConsumeCallback cloneConsumer();
}
