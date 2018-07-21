package z.learn.group;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * initiate new thread to consume based on load
 */
public class KafkaNativeConsumerGroup {

    public void shutdownConsumerGroup() {
        this.stopGroup = true;
    }

    public synchronized boolean addConsumers(List<ConsumeCallback> callbacks) {
        if (getConsumerCount() >= MAX_SIZE) {
            return false;   // reject new callbacks(topic)
        }

        callbacks.forEach(callback -> addConsumer(callback));
        return true;
    }

    private synchronized void addConsumer(ConsumeCallback callback) {
        if (getConsumerCount() >= MAX_SIZE) return;

        NativeConsumerAdaptor adaptor = new NativeConsumerAdaptor();
        consumers.add(adaptor);
        topicConsumerCount.putIfAbsent(callback.getTopic(), 0);
        topicConsumerCount.put(callback.getTopic(), topicConsumerCount.get(callback.getTopic()) + 1);
        adaptor.initAndInvokeConsumer(callback);
    }

    private synchronized void removeConsumer(NativeConsumerAdaptor adaptor) {
        if (getTopicConsumerCount(adaptor.callback.getTopic()) <= 1) return;

        adaptor.stopConsumer = true;
        Iterator<NativeConsumerAdaptor> iterator = consumers.iterator();
        while (iterator.hasNext()) {
            NativeConsumerAdaptor current = iterator.next();
            if (current == adaptor && current.equals(adaptor)) {
                iterator.remove();
                topicConsumerCount.put(current.callback.getTopic(), topicConsumerCount.get(current.callback.getTopic()) - 1);
                break;
            }
        }
    }

    private synchronized int getConsumerCount() {
        return consumers.size();
    }

    private synchronized int getTopicConsumerCount(String topic) {
        return topicConsumerCount.getOrDefault(topic, 0);
    }

    private class NativeConsumerAdaptor {

        private KafkaConsumer<Integer, String> consumer;
        private ConsumeCallback callback;
        private volatile boolean stopConsumer = false;

        private int sequentSuccessfulPoll = 0;
        private int sequentEmptyPoll = 0;
        private static final int SEQUENT_THRESHOLD = 10;

        void initAndInvokeConsumer(ConsumeCallback callback) {
            consumer = new KafkaConsumer<>(defaultConsumerProperties());
            this.callback = callback;
            consumer.subscribe(Arrays.asList(this.callback.getTopic()));

            executor.execute(() -> pollAndInvoke());    // TODO
        }

        void pollAndInvoke() {
            boolean success = false;
            boolean polled = false;
            ConsumerRecords<Integer, String> records = null;
            while (!stopConsumer && !stopGroup) {
                try {
                    success = false;
                    polled = false;
                    if (null == records) {  // new poll
                        records = consumer.poll(DEFAULT_POLL_TIMEOUT);
                        if (null != records && !records.isEmpty()) {
                            polled = true;
                            sequentSuccessfulPoll++;
                            sequentEmptyPoll = 0;
                            if (sequentSuccessfulPoll > SEQUENT_THRESHOLD && getConsumerCount() < MAX_SIZE
                                    && getTopicConsumerCount(callback.getTopic()) < consumer.partitionsFor(callback.getTopic()).size()) {

                                addConsumer(callback.cloneConsumer());
                                sequentSuccessfulPoll = 0;
                            }

                            callback.onMessage(records);
                            success = true;
                        } else {
                            records = null;
                            sequentSuccessfulPoll = 0;
                            sequentEmptyPoll++;

                            if (sequentEmptyPoll > SEQUENT_THRESHOLD * 60 && getConsumerCount() > CORE_SIZE
                                    && getTopicConsumerCount(callback.getTopic()) > 1) {
                                removeConsumer(this);
                                sequentEmptyPoll = 0;
                            }
                        }
                        if (sequentSuccessfulPoll > SEQUENT_THRESHOLD || sequentEmptyPoll > SEQUENT_THRESHOLD * 60)
                            System.out.printf("sequent success [%d], sequent empty [%d]\r\n", sequentSuccessfulPoll, sequentEmptyPoll);

                    } else {
                        polled = true;
                        try {
                            failureStrategy.onConsumeFailure(consumer, records, callback);
                        } catch (Throwable fscst) {
                            // TODO handle strategy failure
                        }
                        records = null;
                    }
                } catch (Throwable throwable) {
                    // TODO handle exception
                } finally {
                    try {
                        if (success) {
                            try {
                                consumer.commitSync();

                            } catch (Throwable ct) {
                                try {
                                    failureStrategy.onCommitFailure(consumer, records);
                                } catch (Throwable fscmt) {
                                    // TODO handle strategy failure
                                }
                            } finally {
                                records = null;
                            }
                        } else if (polled && !success && null == records) { // 重新连接的时候会自动从offset
                            Set<TopicPartition> topicPartitions = records.partitions();
                            for (TopicPartition topicPartition : topicPartitions) {
                                OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition);
                                consumer.seek(topicPartition, offsetAndMetadata.offset());
                            }
                        }
                    } catch (Throwable ft) {
                        ft.printStackTrace();
                    }
                }
            }

            if (stopConsumer || stopGroup) {
                consumer.close();
            }
        }
    }

    private volatile boolean stopGroup = false;
    private List<NativeConsumerAdaptor> consumers = new LinkedList<>();
    private Map<String, Integer> topicConsumerCount = new HashMap<>();
    private FailureStrategy<Integer, String> failureStrategy = new SimpleDiscardFailureStrategy();

    private Properties props = new Properties();

    private Properties defaultConsumerProperties() {
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    private static final int DEFAULT_POLL_TIMEOUT = 100;  // ms

    private AtomicInteger threadCount = new AtomicInteger(0);
    private ThreadPoolExecutor executor =
            new ThreadPoolExecutor(CORE_SIZE, MAX_SIZE, TIME_TO_LIVE, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(1), r -> new Thread(r, THREAD_NAME_PREFIX + threadCount.getAndIncrement()));

    private static final String CONSUMER_GROUP_NAME = "KafkaNativeConsumerGroup";

    private static final String THREAD_NAME_PREFIX = "Kafka_consumer_";
    private static final int CORE_SIZE = 1;
    private static final int MAX_SIZE = 50;
    private static final int TIME_TO_LIVE = 180;
}
