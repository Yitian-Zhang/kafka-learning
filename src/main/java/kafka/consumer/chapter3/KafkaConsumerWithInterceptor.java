package kafka.consumer.chapter3;

import kafka.consumer.config.MyKafkaConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 消费者客户端拦截器Interceptor的使用
 * 配合该consumer的producer为kafka.producer.chapter2.KafkaProducerWithConsumerInterceptor
 *
 * @author yitian
 */
public class KafkaConsumerWithInterceptor {
    private static final String brokerList = "";
    private static final String topic = "";
    private static final String groupId = "";
    private static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static void main(String[] args) {
        // 在配置中加入自定义拦截器
        Properties properties = MyKafkaConsumerConfig.initConfigWithInterceptor(brokerList, groupId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic: " + record.topic() + ", offset: " + record.offset()
                            + ", value: " + record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
