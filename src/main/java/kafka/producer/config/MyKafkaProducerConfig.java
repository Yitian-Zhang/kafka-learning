package kafka.producer.config;

import kafka.producer.interceptor.ProducerInterceptorPrefix;
import kafka.producer.interceptor.ProducerInterceptorPrefixPlus;
import kafka.producer.partitioner.DemoPartitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MyKafkaProducerConfig {

    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-dome";

    public static Properties initBasicConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "client.demo");
        return properties;
    }

    public static Properties initConfigWithPartitioner() {
        Properties properties = initBasicConfig();
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DemoPartitioner.class.getName());
        return properties;
    }

    /**
     * KafkaProducerInterceptor拦截器链
     */
    public static Properties initConfigWithInterceptor() {
        Properties properties = initBasicConfig();
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorPrefix.class.getName() + "," +
                ProducerInterceptorPrefixPlus.class.getName());
        return properties;
    }
}
