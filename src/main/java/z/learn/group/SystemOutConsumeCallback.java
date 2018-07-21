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
        records.forEach(record -> System.out.printf("Thread=[%s], Topic=[%s], Partition=[%d], offset=[%d] ,key=[%d], value=[%s]\r\n",
                Thread.currentThread().getName(), record.topic(), record.partition(), record.offset(), record.key(), record.value()));
        try {
            Thread.sleep(1000 * 2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //throw new RuntimeException("test runtime exception");
    }

    @Override
    public ConsumeCallback cloneConsumer() {
        return this;
    }
}
