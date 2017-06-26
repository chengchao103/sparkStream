package com.huawei.hadoop.security;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.*;

/**
 * Created by Administrator on 2017/6/12.
 */
public class StreamingExampleProducer {
    public static void main(String[] args) {
        String brokerList ="30.3.247.191:21005,30.3.247.192:21005,30.3.247.193:21005,30.3.247.194:21005";
        String topic = "test12345";
                Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig conf = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(conf);

        Random random = new Random();
        String[] names = {"kafka1", "kafka2", "kafka3", "kafka4", "kafka5", "kafka6",
                "kafka7", "kafka8", "kafka9", "kafka10", "kafka11", "kafka12",
                "kafka13", "kafka14", "kafka15", "kafka16", "kafka17",
                "kafka18","kafka19", "kafka20","kafka21", "kafka22","kafka23", "kafka24",
                "kafka25", "kafka26","kafka27"};
        Map<String, String> map = new HashMap<String, String>();
        for (int i = 0; i < names.length / 2; i++) {
            map.put(names[i], "male");
        }
        for (int i = names.length / 3; i < names.length; i++) {
            map.put(names[i], "female");
        }
        for (int m = 0; m < Integer.MAX_VALUE / 2; m ++) {
            List<KeyedMessage<String, String>> dataForMultipleTopics = new ArrayList<KeyedMessage<String, String>>();
            for (int i = 0; i < 10000; i++) {

                String name = names[(i + 314) % names.length];
                String sexy = map.get(name);
                String time = String.valueOf(Math.abs(random.nextInt(3)));

                dataForMultipleTopics.add(new KeyedMessage<String, String>(topic, name + "," + sexy + "," + time));
            }
            producer.send(dataForMultipleTopics);
            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("daddddddddddddddddddd");
        }
    }

    private static void printUsage() {
        System.out.println("Usage: {brokerList} {topic}");
    }
}
