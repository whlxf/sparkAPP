package com.spark.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.ConsumerIterator;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Map;

/**
 * kafka消费者
 */
public class KafkaConsumer extends Thread{

    private String topic;

    public KafkaConsumer(String topic){
        this.topic = topic;
    }

    private ConsumerConnector createConnector(){

        Properties properties = new Properties();
        properties.put("group.id",KafkaProperties.GROUP_ID);
        properties.put("zookeeper.connect",KafkaProperties.ZK);


        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    @Override
    public void run() {
        ConsumerConnector consumerConnector = createConnector();

        Map<String,Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic,1);

        //参数 String topic，List<KafkaStream<byte[],byte[]>> 对应的数据流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0); //get(0)获取每次接收到的数据

        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        while (iterator.hasNext()){
            String message = new String(iterator.next().message());//获取到具体的message
            System.out.println("receive msg: " + message);

            try {
                Thread.sleep(3000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}