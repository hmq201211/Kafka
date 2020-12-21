package helloKafak;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties)) {
            ProducerRecord<String, String> record;
            for (var i = 0; i < 4; i++) {
                record = new ProducerRecord<>("hello", String.valueOf(i), "hello");
                producer.send(record);
                System.out.println(i);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
