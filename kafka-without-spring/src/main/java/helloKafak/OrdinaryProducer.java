package helloKafak;

import entity.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import serializer.UserSerializer;

import java.util.Properties;

public class OrdinaryProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", UserSerializer.class);
        try (KafkaProducer<String, User> producer = new KafkaProducer<String, User>(properties)) {
            ProducerRecord<String, User> record;
            for (int i = 0; i < 4; i++) {
                record = new ProducerRecord<>("hello", String.valueOf(i), new User(i, "name" + i));
                producer.send(record);
                System.out.println(i);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
