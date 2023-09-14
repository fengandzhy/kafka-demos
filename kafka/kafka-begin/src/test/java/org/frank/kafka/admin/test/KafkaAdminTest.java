package org.frank.kafka.admin.test;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.internals.Topic;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaAdminTest {

    public static AdminClient initAdminClient(){
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"47.242.251.45:9092");
        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }
    
    /**
     * 创建topic
     * */
    @Test
    public void createTopicTest(){
        AdminClient adminClient = KafkaAdminTest.initAdminClient();
        NewTopic newTopic = new NewTopic("kafka-sp-topic-1", 2, (short)1);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(newTopic));
        try {
            createTopicsResult.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
