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
import java.util.concurrent.CountDownLatch;

public class KafkaConsumerExmaple {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(KafkaConsumerExmaple.class);
        new KafkaConsumerExmaple().run();
    }

    public  KafkaConsumerExmaple(){

    }

    public  void  run(){

        CountDownLatch latch = new CountDownLatch(1);

        Runnable ts = new ConumserThread("","","",latch);
        Thread myThread = new Thread(ts);

        myThread.start();

        Runtime.getRuntime().addShutdownHook( new Thread(() ->
        {((ConumserThread) ts).shutDown();


            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
            }
        }
        ));


        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {


        }
    }

    public class ConumserThread implements Runnable {


        private  KafkaConsumer<String ,String> kafkaConsumer = null;
        private Logger logger = LoggerFactory.getLogger(KafkaConsumerExmaple.class);

        CountDownLatch latch;

        public ConumserThread(String topicname , String bootstrap ,String groupid , CountDownLatch latch){

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.99.100:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"myid2");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
            KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
            kafkaConsumer.subscribe(Arrays.asList("amittest"));
            kafkaConsumer = new KafkaConsumer<>(properties);
        }
        @Override
        public void run() {

            try {
                while(true){

                    ConsumerRecords<String ,String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    for(ConsumerRecord<String ,String > record :records){
                        logger.info("key = "+record.key());
                        logger.info("value = "+record.value());
                        logger.info("partition = "+record.partition());
                        logger.info("offset  = "+record.offset());

                    }

                    kafkaConsumer.commitSync();
                }
            } catch (Exception e) {
                logger.error("Wake up exception");
            } finally {
                kafkaConsumer.close();
               latch.countDown();

            }

        }

        public  void  shutDown(){
            kafkaConsumer.wakeup();
        }
    }

}
