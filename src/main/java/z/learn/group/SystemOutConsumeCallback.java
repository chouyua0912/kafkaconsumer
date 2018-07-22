package z.learn.group;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public class SystemOutConsumeCallback implements ConsumeCallback<Integer, String> {
    @Override
    public String getTopic() {
        return "test2";
    }

    @Override
    public int getPartition() {
        return 0;
    }

    @Override
    public void onMessage(ConsumerRecords<Integer, String> records) {
        StringBuilder msg = new StringBuilder();
        records.forEach(record -> msg.append(String.format("Thread=[%s], Topic=[%s], Partition=[%d], offset=[%d] ,key=[%d], value=[%s] |",
                Thread.currentThread().getName(), record.topic(), record.partition(), record.offset(), record.key(), record.value())));
        msg.append("-------------------------");

        try {
            Thread.sleep(1000 * 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (Math.random() < 0.3d) {
            throw new RuntimeException("Consumer failure [callback] : " + msg);
        } else {
            System.out.println("Consume succeed [callback] :" + msg);
        }
    }

    @Override
    public ConsumeCallback cloneConsumer() {
        return this;
    }
}
