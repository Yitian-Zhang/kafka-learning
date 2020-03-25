package kafka.consumer.chapter3;

import kafka.consumer.deserializer.CompanyDeserializer;
import kafka.domain.Company;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class CompanyConsumer {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CompanyDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return properties;
    }

    public static void main(String[] args) {
        KafkaConsumer<String, Company> consumer = new KafkaConsumer<String, Company>(initConfig());
        consumer.subscribe(Arrays.asList(topic));
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, Company> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, Company> record : records) {
                    System.out.println("topic: " + record.topic() + ", partition: " + record.partition() +
                            ", offset: " + record.offset());
                    System.out.println("key: " + record.key() + ", value: " + record.value());

                    // do something to process record.
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
