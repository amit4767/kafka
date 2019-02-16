import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerExmapleSeekAssign {

    public static void main(String[] args) {
        String topic = "amittest";

        final Logger logger = LoggerFactory.getLogger(KafkaConsumerExmaple.class);
        Properties properties = new Properties();


        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.99.100:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"myid2");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);


        // if u enable this line also it will fail coz need mutually exclusive
       // kafkaConsumer.subscribe(Arrays.asList(topic));

        TopicPartition partition=  new TopicPartition(topic,0);
        long readOffsetFrom = 15l;
        kafkaConsumer.assign(Arrays.asList(partition));

        kafkaConsumer.seek(partition, readOffsetFrom);


        boolean keepread= true;

        int counter = 0;

        while(keepread){

            ConsumerRecords<String ,String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String ,String > record :records){

                logger.info("key = "+record.key());
                logger.info("value = "+record.value());
                logger.info("partition = "+record.partition());
                logger.info("offset  = "+record.offset());

                 counter = counter +1;

                 if(counter > 5){

                     keepread = false;
                     break;
                 }

            }




        }





    }
}
