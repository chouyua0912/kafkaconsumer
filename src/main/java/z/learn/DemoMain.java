package z.learn;


import z.learn.group.KafkaNativeConsumerGroup;
import z.learn.group.SystemOutConsumeCallback;

import java.util.Collections;

public class DemoMain {

    public static void main(String[] args) throws InterruptedException {
        KafkaNativeProducer producer = new KafkaNativeProducer();
        Thread pThread = new Thread(() -> producer.example(), "NativeProducer");
        pThread.start();

        Thread.sleep(1000);

        KafkaNativeConsumerGroup group = new KafkaNativeConsumerGroup();
        group.addConsumers(Collections.singletonList(new SystemOutConsumeCallback()));

    }
}
