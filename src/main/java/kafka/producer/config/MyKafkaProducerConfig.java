package kafka.producer.config;

import kafka.producer.interceptor.ProducerInterceptorPrefix;
import kafka.producer.interceptor.ProducerInterceptorPrefixPlus;
import kafka.producer.partitioner.DemoPartitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MyKafkaProducerConfig {

    /**
     * Producer基础参数配置
     */
    public static Properties initBasicConfig(String brokerList) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "client.demo");
        return properties;
    }

    /**
     * 配置Producer分区器，可以自定义实现数据分区存放逻辑
     */
    public static Properties initConfigWithPartitioner(String brokerList) {
        Properties properties = initBasicConfig(brokerList);
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DemoPartitioner.class.getName());
        return properties;
    }

    /**
     * 配置KafkaProducerInterceptor拦截器
     */
    public static Properties initConfigWithInterceptor(String brokerList) {
        Properties properties = initBasicConfig(brokerList);
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorPrefix.class.getName());
        return properties;
    }

    /**
     * 配置ProduerInterceptor连接器链
     */
    public static Properties initConfigWithInterceptorChain(String brokerList) {
        Properties properties = initBasicConfig(brokerList);
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorPrefix.class.getName() + "," +
                ProducerInterceptorPrefixPlus.class.getName());
        return properties;
    }

    /**
     * 设置Producer消息发送重试次数
     * @param brokerList Kafka集群host
     * @param retries 重试次数
     */
    public static Properties initConfigWithRetries(String brokerList, Integer retries) {
        Properties properties = initBasicConfig(brokerList);
        properties.put(ProducerConfig.RETRIES_CONFIG, retries);
        return properties;
    }
}
