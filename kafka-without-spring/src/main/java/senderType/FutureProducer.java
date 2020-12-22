package senderType;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class FutureProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties)) {
            ProducerRecord<String, String> record = new ProducerRecord<>("hello", "hello", "world");
            Future<RecordMetadata> send = producer.send(record);
            RecordMetadata recordMetadata = send.get();
            // 阻塞式方法 若是成功则返回元数据 若是失败则抛出异常
            if (recordMetadata != null)
                System.out.println(recordMetadata);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
