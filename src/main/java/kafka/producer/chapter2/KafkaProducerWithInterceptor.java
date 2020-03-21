package kafka.producer.chapter2;

import kafka.producer.config.MyKafkaProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerWithInterceptor {
    public static final String topic = "topic-dome";
    public static final String brokerList = "localhost:9092";

    public static void main(String[] args) {
        // 使用一个interceptor发送消息
        sendRecordUsingInterceptor();

        // 使用拦截器链发送消息
        sendRecordUsingInterceptorChain();

    }

    public static void sendRecordUsingInterceptor() {
        Properties properties = MyKafkaProducerConfig.initConfigWithInterceptor(brokerList);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 3; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "msg-" + i);
            try {
                producer.send(record).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    public static void sendRecordUsingInterceptorChain() {
        Properties properties = MyKafkaProducerConfig.initConfigWithInterceptorChain(brokerList);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 3; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "msg-" + i);
            try {
                producer.send(record).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }


}
