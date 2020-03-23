package kafka.consumer.chapter3;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * KafkaConsumer基础示例代码
 * @author yitian
 */
public class KafkaConsumerAnalysis {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    /**
     * Kafka Consumer配置
     * bootstrap.servers，group.id，key.deserializer，value.deserializer为必填参数
     */
    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo"); // 设置consumer对应的客户端id，如果不设置，kafka会自动设置
        return properties;
    }

    public static void main(String[] args) {
        // 1. 配置消费者客户端参数以及创建相应的消费者实例
        Properties properties = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        // 2. 订阅需要的主题
        consumer.subscribe(Arrays.asList(topic));

        try {
            while (isRunning.get()) {
                // 3. 拉取并消费消息
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic: " + record.topic() + ", partition: " + record.partition() +
                            ", offset: " + record.offset());
                    System.out.println("key: " + record.key() + ", value: " + record.value());

                    // do something to process record.
                }
                // 4. consumer会自动提交消费位移offset
            }
        } catch (Exception e) {
            System.out.println("occer exception: " + e);
        } finally {
            // 5. 关闭消费者实例
            consumer.close();
        }
    }

    /**
     * 不同订阅主题的方式
     */
    public static void subscribeTopic(KafkaConsumer<String, String> consumer) {
        // 1. 使用集合的方式订阅，以最后一次的订阅为准
        consumer.subscribe(Arrays.asList("topic1"));
        consumer.subscribe(Arrays.asList("topic2"));

        // 2. 使用正则表达式的方式订阅，可以订阅满足正则表达式匹配的所有主题
        consumer.subscribe(Pattern.compile("topic-,*"));
    }

    public static void assignPartition(KafkaConsumer<String, String> consumer) {
        // 订阅指定主题中的某个分区(分区编号为0的分区)
        consumer.assign(Arrays.asList(new TopicPartition(topic, 0)));
    }

    /**
     * 订阅指定topic中的所有分区
     */
    public static void assignPartitions(KafkaConsumer<String, String> consumer) {
        List<TopicPartition> partitions = new ArrayList<>();

        // 使用partitionsFor方法可以获取主题中的元数据，来得到该主题的分区信息
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos != null) {
            for (PartitionInfo tpInfo : partitionInfos) {
                partitions.add(new TopicPartition(tpInfo.topic(), tpInfo.partition()));
            }
        }
        consumer.assign(partitions);
    }

    /**
     * 根据ConsumerRecords中的partition然后根据分区进行消费
     * records.records(TopicPartition)方法
     */
    public static void consumerRecordInPartition(KafkaConsumer<String, String> consumer) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

        // 使用records.partitions方法获取拉取消息的所有分区
        for (TopicPartition tp : records.partitions()) {
            // 遍历分区获取每个分区中的消息records方法，根据获取消息中的各个分区分别消费
            for (ConsumerRecord<String, String> record : records.records(tp)) {
                System.out.println(record.partition() + ": " + record.value());
            }
        }
    }

    /**
     * 根据topic列表来消费拉取的消息集合records
     * topic集合需要自己维护
     * records.record(topic)
     */
    public static void consumerRecordInPartitionOfTopic(KafkaConsumer<String, String> consumer) {
        List<String> topicList = Arrays.asList("topic1", "topic2");
        consumer.subscribe(topicList);

        try {
            while (isRunning.get()) {
                // 拉取数据
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                // 根据拉取数据的topic进行消费
                for (String topic : topicList) {
                    for (ConsumerRecord<String, String> record : records.records(topic)) {
                        System.out.println(record.topic() + ": " + record.value());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
