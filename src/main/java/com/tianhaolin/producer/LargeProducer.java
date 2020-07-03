package com.tianhaolin.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class LargeProducer {
    public static void main(String[] args) throws InterruptedException {
        //创建生产者的配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        //序列化类
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,1000);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,1000);
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1000);
        //创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //发送数据
        for (int i = 0; i < 1000; i++) {
            Thread.sleep(10000);
            final int index = i;
            producer.send(new ProducerRecord<String, String>("large","largeTopic"+i), (metadata, exception) -> {
                if (exception == null) {
                    System.out.println(metadata.partition() + "--" + metadata.offset()+"--"+ index);
                } else {
                    exception.printStackTrace();
                }
            });
        }
        System.out.println("tianhaolinTest-1000".getBytes().length);
        //关闭资源
        producer.close();
    }
}
