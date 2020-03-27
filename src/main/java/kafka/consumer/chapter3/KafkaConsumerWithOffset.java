package kafka.consumer.chapter3;

import kafka.consumer.config.MyKafkaConsumerConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka poll方法会自动提交offset，手动提交offset的使用如下
 * 同步提交方式：commitSync()
 * 异步提交方式：commitAsync()
 *
 * @author yitian
 */
public class KafkaConsumerWithOffset {

    private static final String brokerList = "";
    private static final String topic = "";
    private static final String groupId = "";
    private static final AtomicBoolean isRunning = new AtomicBoolean(true);

    /**
     * 无参同步提交offset
     * 配置参数，关闭自动offset提交：properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, false);
     */
    @Test
    public void consumerWithSyncOffset() {
        Properties properties = MyKafkaConsumerConfig.initConfigWithOffset(brokerList, groupId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        while (isRunning.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                // do some logical processing.
            }
            // 无参同步提交offset
            consumer.commitSync();
        }
    }

    /**
     * 无参同步提交offset
     * 消息批量处理和批量offset提交的方式
     */
    @Test
    public void consumerWithSyncOffsetBatch() {
        Properties properties = MyKafkaConsumerConfig.initConfigWithOffset(brokerList, groupId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        final int minBathchSize = 200;
        List<ConsumerRecord> buffer = new ArrayList<>();

        while (isRunning.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBathchSize) {
                // do some logical processing with buffer.

                // commitSync()方法会根据poll方法拉取的最新位移来进行提交（当前消费位移+1的位置），
                // 只要没有发生不可恢复的错误，该方法就会阻塞消费者线程直至位移提交完成。
                consumer.commitSync();
                buffer.clear();
            }
        }
    }

    /**
     * 有参commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets)方法
     * 其中的参数用于提交指定分区的位移。
     * 无参的方法只能提交当前批次对应的position值，如果需要提价一个中间值，比如业务每消费一条记录提交一次位移，则需要使用这种方法。
     */
    @Test
    public void consumerWithSyncPartitionOffset() {
        Properties properties = MyKafkaConsumerConfig.initConfigWithOffset(brokerList, groupId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        while (isRunning.get()) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                // do some logical processing

                long offset = record.offset();
                TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                // 没消费一条记录，提交一次offset（实际上很少使用）
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(offset + 1)));
            }
        }
    }

    /**
     * 按分区粒度同步提交消费位移
     */
    @Test
    public void consumerWithSyncPartitionBatchOffset() {
        Properties properties = MyKafkaConsumerConfig.initConfigWithOffset(brokerList, groupId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        try {
            while (isRunning.get()) {
                // 拉取消息
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                // 根据拉取消息的分区遍历
                for (TopicPartition partition : records.partitions()) {
                    // 获取每个分区中的消息
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    // 根据分区粒度消费消息
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        // do some logical processing.
                    }

                    // 根据分区粒度提交消费位移
                    long lastConsumeOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition,
                            new OffsetAndMetadata(lastConsumeOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }

    }

    /**
     * 异步提交方法commitAsync()
     * 异步提交方式在执行时消费者线程不会被阻塞，可能在提交消费位移的结果还未返回之前就开始了新一次的消息拉取操作。
     */
    @Test
    public void consumerWithAsyncOffset() {
        Properties properties = MyKafkaConsumerConfig.initConfigWithOffset(brokerList, groupId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        while (isRunning.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                // do some logical processing.
            }

            // 异步提交信息时，可以指定callback方法，在位移提交完成后回调onComplete方法
            consumer.commitAsync((map, e) -> {
                if (e == null) {
                    System.out.println(map);
                } else {
                    System.out.println("fail to commit offsets: " + map + "exception: " + e);
                }
            });
        }
    }
}
