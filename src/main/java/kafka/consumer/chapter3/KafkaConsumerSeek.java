package kafka.consumer.chapter3;

import kafka.consumer.config.MyKafkaConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.*;

/**
 * KafkaConsumer中指定offset进行消费
 * seek方法
 */
public class KafkaConsumerSeek {

    private static final String brokerList = "";
    private static final String topic = "";
    private static final String groupId = "";

    /**
     * seek方法的基本使用：
     * consumer.seek(TopicPartition partition, long offset)
     */
    @Test
    public void seekDemo() {
        Properties properties = MyKafkaConsumerConfig.initBasicConfig(brokerList, groupId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        // 1. seek方法之前必须使用一次poll方法，否则会出现异常
        consumer.poll(Duration.ofMillis(1000));
        // 2. 获取consumer消费的所有分区
        Set<TopicPartition> assignment = consumer.assignment();
        for (TopicPartition tp : assignment) {
            // 3. 设置每个分区开始消费的offset=10
            consumer.seek(tp, 10);
        }

        // 实际应用中不能使用while(true)这种方式，使用isRunning或者wakeup方法来控制循环
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            // consume the records.
        }
    }

    /**
     * 上述过程中在seek方法之前需要进行poll方法，但poll方法的timeout时间是很难确定的（需要等待消费者分区分配完成后才可以执行seek方法）
     * 因此为了优化这个问题，下面为第二个seek方法的使用示例
     */
    @Test
    public void seekDemo2() {
        Properties properties = MyKafkaConsumerConfig.initBasicConfig(brokerList, groupId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        // 循环判断assignment是否已经存在值，如果存在即消费者分区已经分配完成，然后在进行seek的操作
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100)); // 拉取消息只是为了判断assignment是否为空，但没有消费消息
            assignment = consumer.assignment();
        }

        for (TopicPartition tp : assignment) {
            consumer.seek(tp, 10);
        }

        // 这里才是真实消费消息的地方
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            // consume the records.
        }
    }

    /**
     * 使用seek方法从分区末尾消息消息
     *
     */
    @Test
    public void seekRecordInLatest() {
        Properties properties = MyKafkaConsumerConfig.initBasicConfig(brokerList, groupId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        Set<TopicPartition> assignment = new HashSet<>();

        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }

        // endOffsets方法用来获取指定分区的末尾消息位置
        Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);
        for (TopicPartition tp : assignment) {
            consumer.seek(tp, offsets.get(tp));
        }
    }

    /**
     * 根据record的时间标记，从指定的时间开始消费
     * consumer.offsetsForTimes()方法:
     *  参数为Map<TopicPartition, OffsetAndTimestamp>，key为待查询的分区，value为待查询的时间戳
     *  返回值为时间戳大于待查询时间的第一条消息对应的位置和时间戳
     */
    @Test
    public void seekRecordForTimes() {
        Properties properties = MyKafkaConsumerConfig.initBasicConfig(brokerList, groupId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        Set<TopicPartition> assignment = new HashSet<>();

        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }

        // 根据时间构建需要查询的分区及时间戳
        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        for (TopicPartition tp : assignment) {
            timestampToSearch.put(tp, System.currentTimeMillis() - 1*24*3600*1000);
        }
        // 调用offsetsForTimes方法
        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampToSearch);
        for (TopicPartition tp : assignment) {
            // 根据分区获取OffsetAndTimestamp对象
            OffsetAndTimestamp offsetAndTimestamp = offsets.get(tp);
            if (offsetAndTimestamp != null) {
                consumer.seek(tp, offsetAndTimestamp.offset());
            }
        }
    }

    /**
     * 将每个分区的消费位移保存到DB中
     * 然后在消费时自己维护消费位移，并可以获取指定位移进行消费
     */
    @Test
    public void seekRecordFromDB() {
        Properties properties = MyKafkaConsumerConfig.initBasicConfig(brokerList, groupId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        Set<TopicPartition> assignment = new HashSet<>();

        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }

        for (TopicPartition tp : assignment) {
            // 从DB中获取指定分区的offset
            long offset = getOffsetFromDB(tp);
            consumer.seek(tp, offset);
        }

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                for (ConsumerRecord<String, String> record : partitionRecords) {
                    // process the record.
                }

                // 计算当前最新消费位移（每个分区）然后存储到DB中
                long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                storeOffsetToDB(partition, lastConsumedOffset);
            }
        }
    }

    private void storeOffsetToDB(TopicPartition partition, long lastConsumedOffset) {
        // store offset of tp to DB
    }

    private long getOffsetFromDB(TopicPartition tp) {
        // get offset of tp from DB
        return 0;
    }

}
