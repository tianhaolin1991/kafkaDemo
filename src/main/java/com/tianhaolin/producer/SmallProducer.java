package com.tianhaolin.producer;

import com.tianhaolin.partitioner.MyPartitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.UniformStickyPartitioner;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;

import java.util.Properties;

public class SmallProducer {
    public static void main(String[] args) throws InterruptedException {
        //创建生产者的配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        //序列化类
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,0);
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, UniformStickyPartitioner.class.getName());
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
        properties.put(ProducerConfig.RETRIES_CONFIG,3);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        //创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //发送数据
        for (int i = 0; i < 10000; i++) {
            /*final int index = i;*/
            Thread.sleep(1000);
            producer.send(new ProducerRecord<String, String>("small","smallTopic-"+i)/*, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println(metadata.partition() + "--" + metadata.offset()+"--"+ index);
                } else {
                    exception.printStackTrace();
                }
            }*/);
        }
        //关闭资源
        producer.close();
    }
}
