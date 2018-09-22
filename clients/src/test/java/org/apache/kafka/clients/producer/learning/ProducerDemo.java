package org.apache.kafka.clients.producer.learning;

/*
 @author:   chenyang
 @date  2018/8/19 下午7:53

*/

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.test.MockMetricsReporter;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo {
    public static void main(String[] args) {
        Properties props=new Properties();
        Boolean isAsync=false;

        props.put(ProducerConfig.CLIENT_ID_CONFIG, "testConstructorClose");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.IntegerSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer=new KafkaProducer<>(props);
        String topic="test";

        int messageNo=1;
        while (true){
            String messageStr="Message_"+messageNo;//消息的value
            long startTime=System.currentTimeMillis();
            if(isAsync){
                producer.send(new ProducerRecord<>(topic,messageNo,messageStr));
                new DemoCallBack(startTime,messageNo,messageStr);
            }else {
                try {
                    RecordMetadata recordMetadata=(RecordMetadata)producer.send(new ProducerRecord<>(topic,messageNo,messageStr)).get();
                    System.out.println("Sent message:("+messageNo+","+messageStr+")");

                }catch (InterruptedException|ExecutionException e){
                    e.printStackTrace();
                }

            }
            ++messageNo;
        }
    }
}

class DemoCallBack implements Callback {
    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key,String message) {
        this.startTime = startTime;
        this.key = key;
        this.message=message;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {

    }
}
