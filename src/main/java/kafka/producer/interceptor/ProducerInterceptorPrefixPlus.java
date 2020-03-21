package kafka.producer.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义ProducerInterceptor2
 */
public class ProducerInterceptorPrefixPlus implements ProducerInterceptor<String, String> {

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        String modifiedValue = "prefix2-" + producerRecord.value();
        return new ProducerRecord<String, String>(producerRecord.topic(),
                producerRecord.partition(),
                producerRecord.timestamp(),
                producerRecord.key(),
                modifiedValue,
                producerRecord.headers());
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
    }

    public void close() {
    }

    public void configure(Map<String, ?> map) {
        // do nothing
    }
}
