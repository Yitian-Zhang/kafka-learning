package kafka.consumer.chapter3.threads;

import kafka.consumer.config.MyKafkaConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * 第一种多线程消费的实现：一个线程对应一个消费者客户端实例，可以称之为消费线程。
 * 一个消费线程可以消费一个或多个分区中的消息，所有的消费线程都隶属于同一个消费group。
 *
 * 注：这种多线程的实现方式和开启多个消费进程没有本质上的区别。
 * 优点是：每个消费线程可以按顺序消费各个分区中的消息。
 * 缺点：每个消费线程都要维护一个独立的TCP连接，如果分区数和ConsumerThreadNum的值都很大，那么会造成不小的系统开销。
 *
 * @author yitian
 */
public class FirstMultiConsumerThreadDemo {

    private static final String brokerList = "";
    private static final String topic = "";
    private static final String groupId = "";

    public static void main(String[] args) {
        Properties properties = MyKafkaConsumerConfig.initBasicConfig(brokerList, groupId);
        // 线程数量，一般不大于topic分区数量，否则会有闲置消费线程
        int consumerThreadNum = 4;

        // 创建多个消费线程并执行消费
        for (int i = 0; i < consumerThreadNum; i++) {
            new KafkaConsumerThread(properties, topic).start();
        }
    }

    /**
     * 消费线程的实现，一个线程对应一个消费者实例
     */
    public static class KafkaConsumerThread extends Thread {
        private KafkaConsumer<String, String> kafkaConsumer;

        public KafkaConsumerThread(Properties properties, String topic) {
            this.kafkaConsumer = new KafkaConsumer<String, String>(properties);
            this.kafkaConsumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records) {
                        // process record.
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.close();
            }
        }
    }
}
