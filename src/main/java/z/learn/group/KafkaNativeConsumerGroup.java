package z.learn.group;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * initiate new thread to consume based on load
 * TODO add cluster control on consumers
 */
public class KafkaNativeConsumerGroup {

    private List<NativeConsumerAdaptor> consumers = new LinkedList<>();
    private Map<String, Integer> topicConsumerCount = new HashMap<>();
    private FailureStrategy<Integer, String> failureStrategy = new SimpleDiscardFailureStrategy();

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
                THREAD_NAME_PREFIX + threadName++);
        worker.start();

        threadCount.getAndIncrement();
        System.out.println("--- Consumer added---- : " + worker.getName());
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
                System.out.println("--- Consumer removed --- : " + Thread.currentThread().getName());
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

        // heartbeat.interval.ms default 3000, session.timeout.ms default 10000
        // max.poll.interval.ms default 300000
        void pollAndInvoke() {
            boolean success = false;
            boolean failOnFailure = false;
            ConsumerRecords<Integer, String> records = null;

            failureStrategy.onStartup(consumer, callback);  //

            while (!stopConsumer && !stopGroup) {
                try {
                    success = false;
                    failOnFailure = false;
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
                            failureStrategy.onConsumeFailure(consumer, records, callback);  // local server in error state, try to recover
                            success = true;
                        } catch (Throwable fscst) {
                            failOnFailure = true;
                            System.err.println("130 exception: " + fscst.getMessage());
                        }
                    }
                } catch (SerializationException se) {
                    se.printStackTrace();
                } catch (Throwable throwable) {
                    success = false;
                    System.err.println("137 exception: " + throwable.getMessage());
                } finally {
                    try {
                        if (success) {
                            handleCommit(records);
                            records = null; // discard records no matter commit successfully or not
                        } else if (failOnFailure) {     // maybe fall in loop
                            seekToLastCommittedPoll(records);    // seek to last poll and poll again
                            records = null;             // keep polling will leave consumer stay in the group. Controlled by max.poll.interval.ms

                            Thread.sleep(FAIL_ON_FAILURE_RETRY_WAIT_TIME);  // TODO should we let consumer to leave the group
                        }
                    } catch (Throwable ft) {
                        ft.printStackTrace();
                    }
                }
            }

            failureStrategy.onShutdown(consumer, callback);

            consumer.close();
        }

        private void checkIfAddWorker() {
            if (sequentSuccessfulPoll >= SEQUENT_COUNT_TO_ADD_WORKER && threadCount.get() < MAX_SIZE
                    && getTopicConsumerCount(callback.getTopic()) < consumer.partitionsFor(callback.getTopic()).size()) {

                addConsumer(callback.cloneConsumer(), consumer.partitionsFor(callback.getTopic()).size());
                sequentSuccessfulPoll = 0;
            }
        }

        private void checkIfRemoveWorker() {
            if (sequentEmptyPoll >= SEQUENT_COUNT_TO_REMOVE_WORKER && threadCount.get() > CORE_SIZE
                    && getTopicConsumerCount(callback.getTopic()) > 1) {
                removeConsumer(this);
                sequentEmptyPoll = 0;
            }
        }

        private void handleCommit(ConsumerRecords<Integer, String> records) {
            try {
                consumer.commitSync();
                System.out.println("-- Committed --" + records.toString());
            } catch (Throwable ct) {
                try {
                    failureStrategy.onCommitFailure(consumer, records); // kafka in unrecoverable state
                } catch (Throwable fscmt) {
                    // TODO handle strategy failure
                }
            }
        }

        private void seekToLastCommittedPoll(ConsumerRecords<Integer, String> records) {
            Set<TopicPartition> topicPartitions = records.partitions();
            for (TopicPartition topicPartition : topicPartitions) {
                OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition);
                if (null == offsetAndMetadata)
                    consumer.seek(topicPartition, 0);
                else
                    consumer.seek(topicPartition, offsetAndMetadata.offset());

                System.out.println("-- Seek to Last Committed --" + topicPartition.toString());
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
        private static final int FAIL_ON_FAILURE_RETRY_WAIT_TIME = 10 * 1000; // ms
    }

    private volatile boolean stopGroup = false;

    public void shutdownConsumerGroup() {
        this.stopGroup = true;
    }

    private Properties props = new Properties();

    private Properties defaultConsumerProperties() {
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100); // 1s = 1000 records, 60000 records a minute
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    private AtomicInteger threadCount = new AtomicInteger(0);
    private int threadName = 0;

    private static final String CONSUMER_GROUP_NAME = "KafkaNativeConsumerGroup";

    private static final String THREAD_NAME_PREFIX = "Kafka_consumer_";
    private static final int CORE_SIZE = 1;
    private static final int MAX_SIZE = 20;
}
