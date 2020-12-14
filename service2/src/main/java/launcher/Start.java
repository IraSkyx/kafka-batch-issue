package launcher;

import io.reactivex.Completable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class Start {
    private static Vertx vertx = Vertx.vertx();

    public static void main(String[] args) {
        //Config
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("group.id", "my_group");
        config.put("auto.offset.reset", "earliest");
        config.put("enable.auto.commit", "true");

        // Create consumer
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(Start.vertx, config);

        // Set up subscription to topic SERVICE2
        Set<String> topics = new HashSet<>();
        topics.add("SERVICE1");

        consumer.subscribe(topics, done -> {
            if(done.succeeded()) {
                System.out.println("Subscribed");

                Start.vertx.setPeriodic(1000, timerId -> {
                    consumer.poll(Duration.ofMillis(100), ar1 -> {
                        if (ar1.succeeded()) {
                            KafkaConsumerRecords<String, String> records = ar1.result();
                            List<Promise> promises = new ArrayList<>();
                            for (int i = 0; i < records.size(); ++i) {
                                KafkaConsumerRecord<String, String> record = records.recordAt(i);
                                promises.add(Start.handleRecord(record));
                            }
                            List<Future> listFutures = promises.stream().map(Promise::future).collect(Collectors.toList());
                            CompositeFuture.join(listFutures).setHandler(ar -> {
                                if(listFutures.size() > 0) {
                                    if(ar.succeeded()) {
                                        System.out.println("OK");
                                    }
                                    else {
                                        System.out.println("KO");
                                    }
                                }
                            });
                        }
                    });

                });
            }
        });
    }

    private static Promise<Void> handleRecord(KafkaConsumerRecord<String, String> rec) {
        Future<Void> futureResult = Start.treatment(rec);
        Promise promise = Promise.promise();
        futureResult.setHandler(ar -> {
            if(ar.succeeded()) {
                promise.complete();
            }
            else {
                promise.fail("Error");
            }
        });
        return promise;
    }

    private static Future<Void> treatment(KafkaConsumerRecord<String, String> rec) {
        Promise promise = Promise.promise();

        System.out.println("key=" + rec.key() + ",value=" + rec.value() + ",partition=" + rec.partition() + ",offset=" + rec.offset());

        WebClient.create(Start.vertx).get(9002, "localhost", "/").send(ar -> {
            if(ar.succeeded()) {
                promise.complete();
            }
            else {
                promise.fail("Error");
            }
        });

        return promise.future();
    }
}
