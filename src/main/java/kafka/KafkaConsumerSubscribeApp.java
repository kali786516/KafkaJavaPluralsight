package kafka;

/**
 * Created by kalit_000 on 5/11/17.
 */
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class KafkaConsumerSubscribeApp {
    public static void main(String[] args){
        //create a properties dictionary for the required/optional Producer config settings:
        Properties props=new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id","test");

        KafkaConsumer myConsumer=new KafkaConsumer(props);

        ArrayList<String> topics=new ArrayList<String>();
        topics.add("my-topic");
        //topics.add("my-other topic");

        myConsumer.subscribe(topics);

        try {
            while (true){
                ConsumerRecords<String,String> records=myConsumer.poll(100);
                for (ConsumerRecord<String,String> record: records){
                    //process each record
                        System.out.println(String.format("Topic : %s, Partition: %d, Offset: %d, Key: %s, Value: %s",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value()
                        ));
                }
                //manual offset management mode
                //myConsumer.commitSync();
                //seek
                //seektobegining
                //seektoend

            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        } finally {
            myConsumer.close();
        }






    }


}
