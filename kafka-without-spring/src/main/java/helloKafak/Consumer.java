package helloKafak;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","127.0.0.1:9092");
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");

        try( KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties)){
            consumer.subscribe(Collections.singletonList("hello"));
            while (true){
                ConsumerRecords<String, String > records = consumer.poll(Duration.ofMillis(400));
                for (ConsumerRecord<String, String>  record: records) {
                    System.out.println(record);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
