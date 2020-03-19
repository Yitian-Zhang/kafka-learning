package kafka.producer.serializer;

import kafka.domain.Company;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 自定义kafkaProducer的value序列化器
 */
public class CompanySerializer implements Serializer<Company> {

    public void configure(Map<String, ?> map, boolean b) {
        // do nothing
    }

    public byte[] serialize(String s, Company company) {
        if (company == null) {
            return null;
        }
        byte[] name, address;
        try {
          if (company.getName() != null) {
              name = company.getName().getBytes("UTF-8");
          } else {
              name = new byte[0];
          }

          if (company.getAddress() != null) {
              address = company.getAddress().getBytes("UTF-8");
          } else {
              address = new byte[0];
          }

          ByteBuffer buffer = ByteBuffer.allocate(4+4+name.length+address.length);
          buffer.putInt(name.length);
          buffer.put(name);
          buffer.putInt(address.length);
          buffer.put(address);
          return buffer.array();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    public void close() {
        // do nothing
    }
}
