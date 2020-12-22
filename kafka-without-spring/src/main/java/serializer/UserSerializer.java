package serializer;

import entity.User;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class UserSerializer implements Serializer<User> {
    @Override
    public byte[] serialize(String s, User user) {
        byte[] name;
        int nameLength;
        if (user == null) {
            return null;
        }
        if (user.getName() != null) {
            name = user.getName().getBytes(StandardCharsets.UTF_8);
            nameLength = name.length;
        } else {
            name = new byte[0];
            nameLength = 0;
        }
        ByteBuffer bytebuffer = ByteBuffer.allocate(4 + 4 + nameLength);
        bytebuffer.putInt(user.getId());
        bytebuffer.putInt(nameLength);
        bytebuffer.put(name);
        return bytebuffer.array();
    }
}
