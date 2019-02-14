import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerExmaple {

    public static void main(String[] args) {
        String topic = "amittest";

        final Logger logger = LoggerFactory.getLogger(KafkaConsumerExmaple.class);
        Properties properties = new Properties();


        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:29092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"myid2");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);


        kafkaConsumer.subscribe(Arrays.asList(topic));


        while(true){

            ConsumerRecords<String ,String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String ,String > record :records){

                logger.info("key = "+record.key());
                logger.info("value = "+record.value());
                logger.info("partition = "+record.partition());
                logger.info("offset  = "+record.offset());

            }




        }





    }
}
