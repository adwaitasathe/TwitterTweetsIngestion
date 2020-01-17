import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();


    }

    public void  run()  {

        // Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        //create a twitter client
       Client client = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();

        //create a kafka Producer

        KafkaProducer<String,String> producer = createKafkaProducer();

       //adding a hutdown hook
        Runtime.getRuntime().addShutdownHook( new Thread(() -> {
            logger.info("Shuuting down application");
            client.stop();
            producer.close();//We do this because so that it sends all msg in memory to kafka

        }));

        //loop to send tweets to kafka

        while (!client.isDone()) {
            String msg = null ;
            try {
              msg  = msgQueue.poll(5, TimeUnit.SECONDS);
            }catch (InterruptedException e) {

                e.printStackTrace();
                client.stop();
            }

            if (msg != null ) {
                logger.info(msg);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("twitter_tweets", null ,msg) ;
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            logger.info("Topic:" + recordMetadata.topic() + "\n" +
                                    "Partition:" + recordMetadata.partition() + "\n" +
                                    "Time:" + recordMetadata.timestamp() + "\n"
                            );
                        } else {
                            logger.error("Error is" + e);
                        }
                    }
                });
            }
        }

    }

    private KafkaProducer<String,String> createKafkaProducer() {
        String bootStapServer = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //creating a safe producer

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION ,"5");

        //creating a high throughput Producer

        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG ,(Integer.toString(32*2014)));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG , "20");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG ,"snappy");

        KafkaProducer<String , String> producer = new KafkaProducer<String  ,String>(properties);

        return producer ;
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        String consumerKey = "f6sxSv3IkOnEyNIwF9Ycayf6i";
        String consumerSecret = "mvVQNtYGDDe5Tv3T3Wwa5SL1GYaqY9PFow91m4jSs0t7bbtltV";
        String token = "3166578447-hoMCQrwmdoeJRj14aoPIwj2G7hWINgdvAmkOv9F";
        String tokenSecret = "FndXsZ7REb8PekqUZMwaxRtHU2ubj3W8Xk9RmoaPGyWsV";



        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("usa" ,"trump");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));


        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
}
