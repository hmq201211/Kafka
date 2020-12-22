package helloKafak;

import entity.User;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import serializer.UserDeserializer;
import serializer.UserSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","127.0.0.1:9092");
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", UserDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");

        try( KafkaConsumer<String, User> consumer = new KafkaConsumer<String, User>(properties)){
            consumer.subscribe(Collections.singletonList("hello"));
            while (true){
                ConsumerRecords<String, User > records = consumer.poll(Duration.ofMillis(400));
                for (ConsumerRecord<String, User>  record: records) {
                    System.out.println(record);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
