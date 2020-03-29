package kafka.consumer.chapter3;

import kafka.consumer.config.MyKafkaConsumerConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka consumer中的在均衡监听器ConsumerRebalanceListener
 * 作用：用来设定发生再均衡动作前后的一些准备或收尾的工作
 */
public class KafkaConsumerRebalanceListener {
    private static final String brokerList = "";
    private static final String topic = "";
    private static final String groupId = "";
    private static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static void main(String[] args) {
        Properties properties = MyKafkaConsumerConfig.initBasicConfig(brokerList, groupId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

        // 使用消费者客户端在均衡监听器，用来设定发生在均衡动作前后的一些准备和收尾工作
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            /**
             * 该方法在再均衡开始之前和消费者停止读取信息之后被调用，可以通过该方法进行位移提交
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                // 同步提交消费位移
                consumer.commitSync(currentOffsets);
                currentOffsets.clear();
            }

            /**
             * 该方法会在重新分配分区之后和消费者开始读取消息之前被调用
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                // do nothing
            }
        });

        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    // process the record.

                    // 将消费者位移暂存到一个局部变量currentOffsets中，这样在正常消费的时候可以通过commitAsync方法来异步提交消费位移，
                    // 在发生在均衡动作之前可以通过再均衡监听器的onPartitionRevoked方法回调执行commitAsync方法通过提交消费位移
                    // 以尽量避免一些不必要的重复消费
                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                }
                consumer.commitAsync(currentOffsets, null);
            }
        } finally {
            consumer.close();
        }
    }
}
