package kafka.producer.chapter2;

import kafka.domain.Company;
import kafka.producer.partitioner.DemoPartitioner;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 生产者客户端示例代码
 */
public class KafkaProducerAnalysis {

    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-dome";

    /**
     * 创建KafkaProducer对象的配置中，有三个参数是必填的：
     * bootstrap.servers
     * key.serializer
     * value.serializer
     */
    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        // 使用自定义分区器
//        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DemoPartitioner.class.getName());
        return properties;
    }

    /**
     * KafkaProducer是线程安全对象，可以在多个线程中共享单个KafkaProducer实例，也可以将KafkaProducer实例进行池化来供其他线程调用
     */
    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 发送消息的三种方式

        // 1. 发送即忘方式
        fireAndForget(producer);

        // 2. 同步发送方式

    }

    /* ProducerRecord发送消息对象的结构：
     *
     * public class ProducerRecord<K, V> {
     *     private final String topic;
     *     private final Integer partition;
     *     private final Headers headers;
     *     private final K key;
     *     private final V value;
     *     private final Long timestamp;
     */

    /**
     * 发送即忘方式，不管消息发送是否成功
     */
    public static void fireAndForget(KafkaProducer<String, String> producer) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "hello, kafka");
        try {
            // send方法本身为异步方式
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 同步发送消息方式1
     */
    public static void syncSend1(KafkaProducer<String, String> producer) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "hello, kafka");
        try {
            // 同步方式1：send方法本身为异步发送方式，这里使用get方法来进行阻塞，等待kafka的响应，知道消息发送成功或者发送异常。
            producer.send(record).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 同步发送消息方式2
     */
    public static void syncSend2(KafkaProducer<String, String> producer) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "hello, kafka");
        try {
            // 同步方式2：也可以在send发送完成后不直接调用get方法，这样可以获得一个RecordMetadata对象
            // 该对象里包含了消息的一些元数据信息，如果后续的代码中需要这些返回的信息，则可以使用如下的方式。
            // 否则使用上面直接调用get方法的同步方式
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            System.out.println(metadata.topic() + "," + metadata.partition() + "," + metadata.offset());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 异步发送消息方式
     */
    public static void asyncSend(KafkaProducer<String, String> producer) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "hello, kafka");
        try {
            // 使用Callback方式进行异步消息发送
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    } else {
                        System.out.println(recordMetadata.topic() + "," + recordMetadata.partition() + "," +
                                recordMetadata.offset());
                    }
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 发送多个消息
     */
    public static void sendManyRecord(KafkaProducer<String, String> producer) {
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "msg-" + i);
            try {
                producer.send(record).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        // close方法会阻塞等待之前所有的请求发送完成后在关闭KafkaProducer，此外还有一个带超时时间的close方法。
        // 如果使用待超时时间的close方法，那么只会在等待timeout时间内来完成所有的尚未完成的请求，然后会强制退出。（实际上很少使用）
        producer.close();
    }
}
