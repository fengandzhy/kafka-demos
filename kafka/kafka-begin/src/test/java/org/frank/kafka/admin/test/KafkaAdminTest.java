package org.frank.kafka.admin.test;

import org.apache.kafka.clients.admin.*;
import org.junit.jupiter.api.Test;

import java.util.*;
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
    
    /**
     * 展示topic 
     * */
    @Test
    public void listTopicTest(){
        AdminClient adminClient = KafkaAdminTest.initAdminClient();
        ListTopicsResult result = adminClient.listTopics();
        try {
            Set<String> topics = result.names().get();
            for(String topic:topics){
                System.out.println(topic);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 删除topic 
     * */
    @Test
    public void deleteTopicTest(){
        AdminClient adminClient = KafkaAdminTest.initAdminClient();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList("xdclass-topic"));
        try {
            deleteTopicsResult.all().get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 展示topic 详细信息
     * */
    @Test
    public void presentTopicDetailTest() throws ExecutionException, InterruptedException {
        AdminClient adminClient = initAdminClient();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList("kafka-sp-topic-1"));

        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();

        Set<Map.Entry<String, TopicDescription>> entries = stringTopicDescriptionMap.entrySet();

        entries.stream().forEach((entry)-> System.out.println("name ："+entry.getKey()+" , desc: "+ entry.getValue()));
    }
    
    /**
     * 增加分区
     * */
    @Test
    public void increasePartitionsTest() throws ExecutionException, InterruptedException {
        Map<String, NewPartitions> infoMap = new HashMap<>();
        NewPartitions newPartitions = NewPartitions.increaseTo(5);

        AdminClient adminClient = initAdminClient();
        infoMap.put("kafka-sp-topic-1", newPartitions);

        CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(infoMap);
        createPartitionsResult.all().get();
    }
}
