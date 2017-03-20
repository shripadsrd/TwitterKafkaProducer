import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

/**
 * Class to read tweets for any given hashtag and produce messages tp a kafka topic.
 */
public class TwitterProducer {

    private static final String TOPIC = "twitter-topic";
    private static final String HASHTAG = "#mondaymotivation";

    public static void run(final String consumerKey, final String consumerSecret,
                           final String token, final String secret) {

        final Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        final ProducerConfig producerConfig = new ProducerConfig(properties);
        final kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
                producerConfig);

        final BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
        final StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        // add some track terms
        endpoint.trackTerms(Lists.newArrayList(HASHTAG));


        final Authentication auth = new OAuth1(consumerKey, consumerSecret, token,
                secret);

        final Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
                .endpoint(endpoint).authentication(auth)
                .processor(new StringDelimitedProcessor(queue)).build();
        client.connect();

        try {
            for (int msgRead = 0; msgRead < 1000; msgRead++) {
                KeyedMessage<String, String> message = null;
                try {
                    message = new KeyedMessage<String, String>(TOPIC, queue.take());
                    System.out.println("*******************************");
                    System.out.println("Message: \n" + message.message());
                    System.out.println("*******************************");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                producer.send(message);
            }
        } finally {
            producer.close();
            client.stop();
        }
    }
    public static void main(final String[] args) {
        try {
            if (args.length == 4) {
                TwitterProducer.run(args[0], args[1], args[2], args[3]);
            } else {
                System.out.println("Four arguments required");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
