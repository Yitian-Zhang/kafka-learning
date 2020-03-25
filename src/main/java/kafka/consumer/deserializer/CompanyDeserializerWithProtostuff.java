package kafka.consumer.deserializer;

import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import kafka.domain.Company;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * 使用通用序列化工具Protostuff来实现Company对象的范反序列化类
 * 使用时需要在consumer中配置value.deserializer为该类的全限定名称
 */
public class CompanyDeserializerWithProtostuff implements Deserializer<Company> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // do nothing
    }

    @Override
    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        Schema schema = RuntimeSchema.getSchema(Company.class);
        Company ans = new Company();
        ProtostuffIOUtil.mergeFrom(data, ans, schema);
        return ans;
    }

    @Override
    public void close() {
        // do nothing
    }
}
