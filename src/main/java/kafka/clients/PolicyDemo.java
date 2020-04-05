package kafka.clients;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;

import java.util.Map;

/**
 * 使用KafkaAdminClient创建topic时，设置的合法性验证规则
 *
 * 该类创建完成之后，需要在kafka的config/server.properties配置文件中进行如下配置：
 * create.topic.policy.class.name = kafka.clients.PolicyDemo
 *
 * @author yitian
 */
public class PolicyDemo implements CreateTopicPolicy {

    /**
     * 这里设置的合法性验证规则为：topic分区数不小于5，副本因子不小于2
     * @param requestMetadata
     * @throws PolicyViolationException
     */
    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        if (requestMetadata.numPartitions() != null || requestMetadata.replicationFactor() != null) {
            if (requestMetadata.numPartitions() < 5) {
                throw new PolicyViolationException("Topic should have at least 5 partitions, received: " +
                        requestMetadata.numPartitions());
            }
            if (requestMetadata.replicationFactor() <= 1) {
                throw new PolicyViolationException("Topic should have at least 2 replication factor, received: " +
                        requestMetadata.replicationFactor());
            }
        }
    }

    @Override
    public void close() throws Exception {
        // do nothing
    }

    @Override
    public void configure(Map<String, ?> map) {
        // do nothing
    }
}
