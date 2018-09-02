package com.spark.kafka;

/**
 * kafka测试类
 */
public class KafkaClientApp {
    public static void main(String[] args) {
        /*
        本地启动程序，测试：节点上启动consumer进程，查看消费过程
         */
        new KafkaProducer(KafkaProperties.TOPIC).start();

        new KafkaConsumer(KafkaProperties.TOPIC).start();
    }
}
