package kafka.clients;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * 使用KafkaAdminClient来管理Kafka
 *
 * kafka 0.11.0.0 版本之后，Kafka提供了工具类KafkaAdminClient来替代AdminClient，
 * 用于对broker、配置和ACL（Access Control List），topic的管理
 *
 * @author yitian
 */
public class KafkaAdminClientsDemo {

    private static final String brokerList = "localhost:9092";
    private static final String topic = "topic-admin";

    /**
     * 构建AdminClient的创建配置对象
     */
    private Properties initAdminClientConfig() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        return properties;
    }

    /**
     * 使用NewTopic指定分区数和副本因子创建Topic
     */
    @Test
    public void createTopicDemo1() {
        // 1. 根据Config创建AdminConfig对象，实际上是创建KafkaAdminClient子类对象
        AdminClient client = AdminClient.create(initAdminClientConfig());

        // 2. 使用NewTopic设定所要创建的主题的具体信息，包含创建主题是需要的主题名称、分区数和副本因子信息
        NewTopic newTopic = new NewTopic(topic, 4, (short) 1);
        // 3. 真正的创建主题的核心步骤
        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));

        try {
            // 4. 创建主题的返回值为VOID
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        // 5. 使用完成KafkaAdminClient对象之后，最后需要调用close方法释放资源
        client.close();
    }

    /**
     * 使用NewTopic中指定分区副本的分配方式来直接创建Topic
     */
    @Test
    public void createTopicDemo2() {
        AdminClient client = AdminClient.create(initAdminClientConfig());

        // 直接构建分区副本的分配方式
        Map<Integer, List<Integer>> replicasAssignments = new HashMap<>();
        replicasAssignments.put(0, Arrays.asList(0));
        replicasAssignments.put(1, Arrays.asList(0));
        replicasAssignments.put(2, Arrays.asList(0));
        replicasAssignments.put(3, Arrays.asList(0));

        // 使用NewTopic的重载构造方法创建新Topic
        NewTopic newTopic = new NewTopic(topic, replicasAssignments);
        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));

        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    /**
     * 在创建Topic的同时，指定topic的一些config配置项
     */
    @Test
    public void createTopicWithConfig() {
        AdminClient client = AdminClient.create(initAdminClientConfig());

        NewTopic newTopic = new NewTopic(topic, 4, (short) 1);
        // 加入需要配置的配置项
        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy", "compact");
        newTopic.configs(configs);

        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));

        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    /**
     * 显示topic的列表信息（自己写的，待验证）
     * 类似的还有：deleteTopics， ListTopics
     */
    @Test
    public void listTopics() {
        AdminClient client = AdminClient.create(initAdminClientConfig());

        ListTopicsResult result = client.listTopics();
        try {
            result.listings().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    /**
     * 列出topic的所有描述信息
     */
    @Test
    public void describeTopics() {
        AdminClient client = AdminClient.create(initAdminClientConfig());

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        DescribeConfigsResult result = client.describeConfigs(Collections.singleton(resource));

        try {
            Config config = result.all().get().get(resource);
            System.out.println(config);

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    /**
     * 修改指定主题的配置参数
     */
    @Test
    public void alertTopics() {
        AdminClient client = AdminClient.create(initAdminClientConfig());

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        ConfigEntry entry = new ConfigEntry("cleanup.policy", "compact");

        Config config = new Config(Collections.singleton(entry));
        Map<ConfigResource, Config> configs = new HashMap<>();
        configs.put(resource, config);

        AlterConfigsResult result = client.alterConfigs(configs);
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    /**
     * 增加指定topic分区
     */
    @Test
    public void addPartitionsForTopic() {
        AdminClient client = AdminClient.create(initAdminClientConfig());

        NewPartitions newPartitions = NewPartitions.increaseTo(5);
        Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
        newPartitionsMap.put(topic, newPartitions);

        CreatePartitionsResult result = client.createPartitions(newPartitionsMap);
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }
}
