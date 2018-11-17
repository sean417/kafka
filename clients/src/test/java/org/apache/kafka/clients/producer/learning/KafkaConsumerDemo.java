package org.apache.kafka.clients.producer.learning;

/*
 @author:   chenyang
 @date  2018/11/8 5:14 PM

*/

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerDemo {

    public static void main(String[] args) {
        Properties props=new Properties();
        props.put("bootstrap.servers","localhost:9092");//broker地址
        props.put("group.id","test");//所属的Consumer Group的Id
        props.put("enable.auto.commit","true");
        //自动提交offset的时间间隔
        props.put("auto.commit.interval.ms","1000");
        props.put("session.timeout.ms","30000");
        //key使用的Deserializer
        props.put("key.deserializer","org.apache.kafka.common.serialization.IntegerSerializer");
        // value 使用的Deserializer
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(props);
        //订阅test1和test2两个topic
        consumer.subscribe(Arrays.asList("test1","test2"));
        try{
            while (true){
                //从服务端拉取消息，每次poll（）可以拉取多个消息
                ConsumerRecords<String,String> records=consumer.poll(100);
                //消费消息，这里仅仅是将消息的offset，key,value输出
                for(ConsumerRecord<String,String> record:records){
                    System.out.printf("offset= %d,key= %s,value= %s\n",record.offset(),record.key(),record.value());
                }
            }
        }finally {
                consumer.close();
        }


    }
}
