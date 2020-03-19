package kafka.producer.chapter2;

import kafka.domain.Company;
import kafka.producer.serializer.CompanySerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 指定KafkaProducer中的value Serializer来发送Company数据
 */
public class CompanyProducer {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-dome";

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 指定value序列化器（自定义的）
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        return properties;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaProducer<String, Company> producer = new KafkaProducer<String, Company>(initConfig());
        Company company = Company.builder().name("Kafka").address("China").build();

        ProducerRecord<String, Company> record = new ProducerRecord<String, Company>(topic, company);
        producer.send(record).get();

    }
}
