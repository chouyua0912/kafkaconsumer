package z.learn.group;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * initiate new thread to consume based on load
 */
public class KafkaNativeConsumerGroup {

    private List<NativeConsumerAdaptor> consumers = new LinkedList<>();
    private Map<String, Integer> topicConsumerCount = new HashMap<>();

    public synchronized boolean addConsumers(List<ConsumeCallback> callbacks) { // need to keep the consumers less than the partitions
        if (threadCount.get() >= MAX_SIZE
                || callbacks.size() + threadCount.get() > MAX_SIZE) {
            return false;   // reject new callbacks(topic)
        }

        callbacks.forEach(callback -> addConsumer(callback, 1));
        return true;
    }

    private synchronized void addConsumer(ConsumeCallback callback, int partitionCount) {   // partition control count
        if (threadCount.get() >= MAX_SIZE ||
                topicConsumerCount.getOrDefault(callback.getTopic(), 0) >= partitionCount) return;

        NativeConsumerAdaptor adaptor = new NativeConsumerAdaptor(callback);
        consumers.add(adaptor);
        topicConsumerCount.putIfAbsent(callback.getTopic(), 0);
        topicConsumerCount.put(callback.getTopic(), topicConsumerCount.get(callback.getTopic()) + 1);

        Thread worker = new Thread(() -> adaptor.pollAndInvoke(),
                THREAD_NAME_PREFIX + threadCount.getAndIncrement());
        worker.start();
    }

    private synchronized void removeConsumer(NativeConsumerAdaptor adaptor) {
        if (threadCount.get() < CORE_SIZE
                || getTopicConsumerCount(adaptor.callback.getTopic()) <= 1) return;

        Iterator<NativeConsumerAdaptor> iterator = consumers.iterator();
        while (iterator.hasNext()) {
            NativeConsumerAdaptor current = iterator.next();
            if (current == adaptor && current.equals(adaptor)) {

                iterator.remove();
                adaptor.stopConsumer = true;
                topicConsumerCount.put(current.callback.getTopic(), topicConsumerCount.get(current.callback.getTopic()) - 1);
                threadCount.getAndDecrement();
                break;
            }
        }
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

        NativeConsumerAdaptor(ConsumeCallback callback) {
            this.callback = callback;
            consumer = new KafkaConsumer<>(defaultConsumerProperties());
            consumer.subscribe(Arrays.asList(this.callback.getTopic()));    // handle exception
        }

        void pollAndInvoke() {
            boolean success = false;
            boolean failureOnFailure = false;
            ConsumerRecords<Integer, String> records = null;
            while (!stopConsumer && !stopGroup) {
                try {
                    success = false;
                    failureOnFailure = false;
                    if (null == records) {  // new poll
                        records = consumer.poll(DEFAULT_POLL_TIMEOUT);
                        if (null != records && !records.isEmpty()) {
                            if (sequentSuccessfulPoll < SEQUENT_COUNT_TO_ADD_WORKER)
                                sequentSuccessfulPoll++;

                            sequentEmptyPoll = 0;
                            checkIfAddWorker();

                            callback.onMessage(records);
                            success = true;
                        } else {            // empty polled
                            records = null; // discard empty poll result

                            sequentSuccessfulPoll = 0;
                            if (sequentEmptyPoll < SEQUENT_COUNT_TO_REMOVE_WORKER)  // in case of overflow
                                sequentEmptyPoll++;
                            checkIfRemoveWorker();
                        }
                        debugLog();

                    } else {    // handle consume failure
                        try {
                            failureStrategy.onConsumeFailure(consumer, records, callback);  // local server in error state
                        } catch (Throwable fscst) {
                            // TODO handle strategy failure
                            failureOnFailure = true;
                        }
                    }
                } catch (Throwable throwable) {
                    // TODO handle exception
                    success = false;
                } finally {
                    try {
                        if (success) {
                            handleCommit(records);
                            records = null; // discard records no matter commit successfully or not
                        } else if (failureOnFailure) {  // maybe fall in loop, try to recover from local error
                            seekToLastPoll(records);
                            records = null;
                        }
                    } catch (Throwable ft) {
                        ft.printStackTrace();
                    }
                }
            }

            consumer.close();
        }

        private void checkIfAddWorker() {
            if (sequentSuccessfulPoll > SEQUENT_COUNT_TO_ADD_WORKER && threadCount.get() < MAX_SIZE
                    && getTopicConsumerCount(callback.getTopic()) < consumer.partitionsFor(callback.getTopic()).size()) {

                addConsumer(callback.cloneConsumer(), consumer.partitionsFor(callback.getTopic()).size());
                sequentSuccessfulPoll = 0;
            }
        }

        private void checkIfRemoveWorker() {
            if (sequentEmptyPoll > SEQUENT_COUNT_TO_REMOVE_WORKER && threadCount.get() > CORE_SIZE
                    && getTopicConsumerCount(callback.getTopic()) > 1) {
                removeConsumer(this);
                sequentEmptyPoll = 0;
            }
        }

        private void handleCommit(ConsumerRecords<Integer, String> records) {
            try {
                consumer.commitSync();
            } catch (Throwable ct) {
                try {
                    failureStrategy.onCommitFailure(consumer, records); // kafka in unrecoverable state
                } catch (Throwable fscmt) {
                    // TODO handle strategy failure
                }
            }
        }

        private void seekToLastPoll(ConsumerRecords<Integer, String> records) {
            Set<TopicPartition> topicPartitions = records.partitions();
            for (TopicPartition topicPartition : topicPartitions) {
                OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition);
                consumer.seek(topicPartition, offsetAndMetadata.offset());
            }
        }

        private void debugLog() {
            if (sequentSuccessfulPoll > SEQUENT_COUNT_TO_ADD_WORKER
                    || sequentEmptyPoll > SEQUENT_COUNT_TO_REMOVE_WORKER)
                System.out.printf("sequent success [%d], sequent empty [%d]\r\n", sequentSuccessfulPoll, sequentEmptyPoll);
        }

        private static final int DEFAULT_POLL_TIMEOUT = 100;  // ms
        private static final int SEQUENT_COUNT_TO_ADD_WORKER = 10;  // 10 * 100ms = 1s
        private static final int SEQUENT_COUNT_TO_REMOVE_WORKER = SEQUENT_COUNT_TO_ADD_WORKER * 60; // 1s * 60 = 1m
    }

    private volatile boolean stopGroup = false;

    public void shutdownConsumerGroup() {
        this.stopGroup = true;
    }

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

    private AtomicInteger threadCount = new AtomicInteger(0);

    private static final String CONSUMER_GROUP_NAME = "KafkaNativeConsumerGroup";

    private static final String THREAD_NAME_PREFIX = "Kafka_consumer_";
    private static final int CORE_SIZE = 1;
    private static final int MAX_SIZE = 20;
}
