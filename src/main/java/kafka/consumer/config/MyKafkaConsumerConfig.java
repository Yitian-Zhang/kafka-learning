package kafka.consumer.config;

import kafka.consumer.interceptor.ConsumerInterceptorTTL;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class MyKafkaConsumerConfig {

    public static Properties initBasicConfig(String brokerList, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return properties;
    }

    public static Properties initConfigWithOffset(String brokerList, String groupId) {
        Properties properties = initBasicConfig(brokerList, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, false);
        return properties;
    }

    public static Properties initConfigWithInterceptor(String brokerList, String groupId) {
        Properties properties = initBasicConfig(brokerList, groupId);
        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerInterceptorTTL.class.getName());
        return properties;
    }


}
