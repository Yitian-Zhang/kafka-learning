package kafka.producer.serializer;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import kafka.domain.Company;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * 使用Protostuff（通用序列化工具）实现Company消息的序列化
 *
 * @author yitian
 */
public class CompanySerializerWithProtostuff implements Serializer<Company> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // do nothing
    }

    @Override
    public byte[] serialize(String topic, Company data) {
        if (data == null) {
            return null;
        }
        Schema schema = RuntimeSchema.getSchema(data.getClass());
        LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        byte[] protosuff = null;

        try {
            protosuff = ProtostuffIOUtil.toByteArray(data, schema, buffer);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            buffer.clear();
        }
        return protosuff;
    }

    @Override
    public void close() {
        // do nothing
    }
}
