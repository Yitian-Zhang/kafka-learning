package kafka.producer.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义ProducerInterceptor
 */
public class ProducerInterceptorPrefix implements ProducerInterceptor<String, String> {
    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    /**
     * onSend方法中用于实现消息的定制化操作
     * @param producerRecord 发送是消息
     * @return 操作后的消息对象
     */
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        String modifiedValue = "prefix1-" + producerRecord.value();
        return new ProducerRecord<String, String>(producerRecord.topic(),
                producerRecord.partition(),
                producerRecord.timestamp(),
                producerRecord.key(),
                modifiedValue,
                producerRecord.headers());
    }

    /**
     * KafkaProducer会在消息被ACK之前或消息发送失败时调用这里的onAcknowledge方法，该方法的执行优先于用户设定的Callback回调方法。
     * @param recordMetadata 发送后得到的返回值原信息
     * @param e 异常信息
     */
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            sendSuccess++;
        } else {
            sendFailure++;
        }
    }

    /**
     * close方法用于在关闭拦截器时执行一些资源释放的清理工作
     */
    public void close() {
        double successRatio = (double) sendSuccess / (sendFailure + sendSuccess);
        System.out.println("[INFO] 数据发送的成功率为：" + String.format("%f", successRatio * 100) + "%");
    }

    /**
     * Interceptor和Partitioner接口一样，用于公共的父接口Configurable，具有configure方法
     * @param map
     */
    public void configure(Map<String, ?> map) {
        // do nothing
    }
}
