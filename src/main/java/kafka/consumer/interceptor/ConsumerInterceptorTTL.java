package kafka.consumer.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 自定义消费者拦截器（Interceptor）
 * implements ConsumerInterceptor接口
 *
 * 该拦截的作用：
 * 使用消费者拦截器实现一个简单的消息TTL（Time to Live）的功能，在过滤在规定的时间间隔内无法到达的消息。
 * 具体的，该消费者拦截器使用消息的timestamp字段来判断消息是否过期，如果消息的时间戳与当前系统时间戳相差超过10s，
 * 则认为该消息已经过期，那么这条消息就会被过滤掉，而不投递给具体的消费者。
 *
 * @author yitian
 */
public class ConsumerInterceptorTTL implements ConsumerInterceptor<String, String> {
    private static final long EXPIRE_INTERVAL = 10 * 1000;

    /**
     * onConsume方法会在poll方法返回之前被调用，来对消息进行定制化操作，比如修改返回的消息内容、按照某种规则过滤消息（可能会减少poll方法
     * 的返回的消息个数）。
     * @param consumerRecords 通过poll方法获取的消息集合
     * @return 拦截器处理过后的消息集合
     */
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> consumerRecords) {
        long now = System.currentTimeMillis();
        Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecords = new HashMap<>();

        for (TopicPartition tp : consumerRecords.partitions()) {
            List<ConsumerRecord<String, String>> tpRecords = consumerRecords.records(tp);

            List<ConsumerRecord<String, String>> newTpRecords = new ArrayList<>();
            for (ConsumerRecord<String, String> record : tpRecords) {
                if (now - record.timestamp() < EXPIRE_INTERVAL) {
                    newTpRecords.add(record);
                }
            }
            if (!newTpRecords.isEmpty()) {
                newRecords.put(tp, newTpRecords);
            }
        }
        return new ConsumerRecords<>(newRecords);
    }

    /**
     * onCommit方法在提交完消费位移之后进行调用，可以使用这个方法来记录跟踪所提交的消息位移信息，
     * 比如当消费者使用commitSync的无参方法时，我们不知道提交的消费位移细节，此时可以使用该方法做到这一点
     * @param offsets
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((tp, offset) -> System.out.println(tp + ": " + offset.offset()));
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public void configure(Map<String, ?> map) {
        // do nothing
    }
}
