
import org.apache.kafka.clients.producer.*;

import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KakfaProducerTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        //properties.setProperty("bootstrap.servers","192.168.99.100:9092");

        final Logger logger = LoggerFactory.getLogger(KakfaProducerTest.class);
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.99.100:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("retries","3");
        properties.setProperty("acks","1");


        //Safe producer .

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"1");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        KafkaProducer<String ,String> producer = new KafkaProducer<String, String>(properties) ;

        //Higher through put setting at the bit cpu usage n latency .
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));



        for(int i = 0 ; i< 10 ;i++){

            String topic = "amittest";
            String value = "value"+i;
            String key = "value"+i;

            ProducerRecord<String ,String> test = new ProducerRecord<String ,String>(topic,key,value);
            ///producer.flush();
            producer.send(test, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if(e ==null){
                        logger.info("recordMedata ="+recordMetadata.topic());
                        logger.info("recordMedata ="+recordMetadata.offset());
                        logger.info("recordMedata ="+recordMetadata.partition());
                    }else
                        logger.error("error ="+e);

                }
            }).get();


        }


        producer.flush();
        producer.close();
        //  nK98fOK_Qo-ppqZnwo3GMw      //


    }
}
