package kafka.consumer.chapter3.threads;

import kafka.consumer.config.MyKafkaConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 第三种多线程消费者实现方式（第二种因为不常用没有具体的实现实例）
 * 通过消费者线程池的方式（KafkaConsumerThread），为每次poll方法获取的records分配一个线程（RecordsHandler）进行消息的具体处理。
 *
 * @author yitian
 */
public class ThirdMultiConsumerThreadDemo {
    private static final String brokerList = "localhost:9092";
    private static final String topic = "topic-demo";
    private static final String groupId = "group.demo";

    public static void main(String[] args) {
        Properties properties = MyKafkaConsumerConfig.initBasicConfig(brokerList, groupId);
        KafkaConsumerThread consumerThread = new KafkaConsumerThread(properties, topic,
                Runtime.getRuntime().availableProcessors());
        consumerThread.start();
    }

    /**
     * 消费者线程池
     * 为每次poll方法获取的records开启一个线程（RecordsHandler来进行records的具体处理）
     */
    public static class KafkaConsumerThread extends Thread {
        private KafkaConsumer<String, String> kafkaConsumer;
        private ExecutorService executorService;
        private int threadNumber;

        public KafkaConsumerThread(Properties properties, String topic, int threadNumber) {
            this.kafkaConsumer = new KafkaConsumer<String, String>(properties);
            this.kafkaConsumer.subscribe(Arrays.asList(topic));
            this.threadNumber = threadNumber;
            executorService = new ThreadPoolExecutor(
                    threadNumber,
                    threadNumber,
                    0L,
                    TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(1000),
                    new ThreadPoolExecutor.CallerRunsPolicy());
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    if (!records.isEmpty()) {
                        executorService.submit(new RecordsHandler(records));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.close();
            }
        }
    }

    /**
     * 消息处理的具体线程
     */
    public static class RecordsHandler extends Thread {
        public final ConsumerRecords<String, String> records;

        public RecordsHandler(ConsumerRecords<String, String> records) {
            this.records = records;
        }

        @Override
        public void run() {
            for (ConsumerRecord<String, String> record : records) {
                // process the records.
            }
        }
    }
}
