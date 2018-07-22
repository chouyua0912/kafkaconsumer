package z.learn.group;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

class SimpleDiscardFailureStrategy implements FailureStrategy<Integer, String> {

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
    public void onConsumeFailure(KafkaConsumer<Integer, String> consumer, ConsumerRecords<Integer, String> records,
                                 ConsumeCallback<Integer, String> callback) {

        StringBuilder msg = new StringBuilder();

        records.forEach(record -> msg.append(String.format("Thread=[%s], Topic=[%s], Partition=[%d], offset=[%d] ,key=[%d], value=[%s] |",
                Thread.currentThread().getName(), record.topic(), record.partition(), record.offset(), record.key(), record.value())));
        msg.append("-------------------");
        if (Math.random() > 0.3d) {
            System.out.println("Consumer failure recovered succeed [Strategy] : " + msg);
        } else {
            throw new RuntimeException("Consumer failure recovered failed [Strategy] : " + msg);
        }
    }
}
