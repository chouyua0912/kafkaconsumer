package z.learn;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaNativeProducer {

    public void example() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", IntegerSerializer.class);
        props.put("value.serializer", StringSerializer.class);

        Producer<Integer, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 2000; i++)
            producer.send(new ProducerRecord<>("test2", Integer.valueOf(i), "produced:--[ " + Integer.toString(i)));

        producer.close();
    }
}
