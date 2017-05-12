package kafka;

/**
 * Created by kalit_000 on 5/11/17.
 */
//main class
import org.apache.kafka.clients.producer.KafkaProducer;
// data sent via producer record
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;


public class KafkaSimpleProducer {
    public static void main(String[] args){

        //create a properties dictornary for the required/optional producer confid settings:
        Properties props=new Properties();
        props.put("bootstrap.servers","localhost:9092,localhost:9093");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //-->

        //define main kafka producer class
        // key and value should be string as per below
        KafkaProducer<String,String> myproducer=new KafkaProducer<String, String>(props);

        try{
            for(int i=0;i < 150; i++){
                //key value inside producerRecord
                // my-topic is my topic name
                myproducer.send(new ProducerRecord<String, String>("my-topic",Integer.toString(i),"MyMessage: "+Integer.toString(i)));
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            // gracefully close down producer
            myproducer.close();
        }
    }


}
