package kafka.producer.chapter2;

import kafka.producer.config.MyKafkaProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 配合ConsumerInterceptorTTL消费者客户端拦截器使用的生产者
 *
 * @author yitian
 */
public class KafkaProducerWithConsumerInterceptor {
    private static final String brokerList = "";
    private static final String topic = "";
    private static final long EXPIRE_INTERVAL = 10 * 1000;


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = MyKafkaProducerConfig.initBasicConfig(brokerList);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 1. 同步发送第一个消息，设置该消息的时间戳来使其变得超时
        ProducerRecord<String, String> record1 = new ProducerRecord<>(topic, 0,
                System.currentTimeMillis() - EXPIRE_INTERVAL, null, "first-expire-data");
        producer.send(record1).get();

        // 2. 同步发送第二个消息，设置该消息的时间戳为正常
        ProducerRecord<String, String> record2 = new ProducerRecord<>(topic, 0,
                System.currentTimeMillis(), null, "normal-data");
        producer.send(record2).get();

        // 3. 同步发送第三个消息，设置该消息的时间戳使其变的超时
        ProducerRecord<String, String> record3 = new ProducerRecord<>(topic, 0,
                System.currentTimeMillis() - EXPIRE_INTERVAL, null, "last-expire-data");
        producer.send(record3).get();
    }
}
