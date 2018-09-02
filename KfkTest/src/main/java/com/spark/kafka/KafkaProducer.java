package com.spark.kafka;

import kafka.javaapi.producer.Producer;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * kafka生产者
 */
public class KafkaProducer extends Thread{

    private String topic;
    private Producer<Integer,String> producer;

    public KafkaProducer(String topic){
        this.topic = topic;

        Properties properties = new Properties();
        properties.put("metadata.broker.list",KafkaProperties.BROKER_LIST);
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("request.required.acks",1);

        producer = new Producer<Integer, String>(new ProducerConfig(properties));
    }

    @Override
    public void run() {

        int msgNum = 1;
        while (true){
            String msg = "message-000" + msgNum;
            producer.send(new KeyedMessage<Integer, String>(topic,msg));
            System.out.println("send: " + msg);
            msgNum++;

            try {
                Thread.sleep(3000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
