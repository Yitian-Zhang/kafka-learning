package kafka.producer.chapter2;

import kafka.producer.config.MyKafkaProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerWithPartitioner {
    private static final String brokerList = "localhost:9092";
    private static final String topic = "topic-demo";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = MyKafkaProducerConfig.initConfigWithPartitioner(brokerList);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 3; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "msg-" + i);
            producer.send(record).get();
        }
        producer.close();
    }
}
