package org.frank.kafka.consumer;

import java.util.Properties;

public class ConsumerTest {
    
    public static Properties getProperties() {
        Properties props = new Properties();

        //broker地址
        props.put("bootstrap.servers", "112.74.55.160:9092");

        //消费者分组ID，分组内的消费者只能消费该消息一次，不同分组内的消费者可以重复消费该消息
        props.put("group.id", "xdclass-g1");

        //开启自动提交offset
        props.put("enable.auto.commit", "true");

        //自动提交offset延迟时间
        props.put("auto.commit.interval.ms", "1000");

        //反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return props;
    }
}
