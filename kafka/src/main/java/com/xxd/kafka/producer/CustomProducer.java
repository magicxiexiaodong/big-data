package com.xxd.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class CustomProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop113:9092,hadoop114:9092,hadoop115:9092");
        props.put("acks", "all");
        props.put("retries", 1);
        props.put("batch.size", 10);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<Object, Object> producer = new KafkaProducer<>(props);

        // 不带回调的函数
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord("first", Integer.toString(i), Integer.toString(i)));
        }
        // 带回调的函数
     /*   for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>("first", Integer.toString(i), Integer.toString(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("success->" + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }*/
        producer.close();
    }
}
