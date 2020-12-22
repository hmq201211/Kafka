package serializer;

import entity.User;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class UserDeserializer implements Deserializer<User> {
    @Override
    public User deserialize(String s, byte[] bytes) {
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        int id = wrap.getInt();
        int length = wrap.getInt();
        byte[] nameByte = new byte[length];
        wrap.get(nameByte);
        return new User(id, new String(nameByte, StandardCharsets.UTF_8));
    }
}
