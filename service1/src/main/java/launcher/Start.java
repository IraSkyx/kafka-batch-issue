package launcher;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.util.HashMap;
import java.util.Map;

public class Start {
    public static void main(String[] args) {
        //Config
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("acks", "1");

        Vertx vertx = Vertx.vertx();
        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, config);

        try {
            for (int i = 0; i < 10; ++i) {
                KafkaProducerRecord<String, String> recordProduct = KafkaProducerRecord.create("SERVICE1", "SERVICE1_PAYLOAD");
                producer.write(recordProduct);
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        finally {
            //producer.close();
        }
    }
}
