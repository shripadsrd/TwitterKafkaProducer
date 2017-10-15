The class TwitterProducer is used to consume messages from any given hashtag and write to a Kafka topic.

Steps to make this producer work:
1. Start Zookeeper using the command `zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties`, confirm using `netstat -anl | grep 2181`

2. Start Kafka server using the command `kafka-server-start /usr/local/etc/kafka/server.properties`, confirm using `netstat -anl | grep 9092`

3. Run the TwitterProducer class with the following arguments: Consumer Key, Consumer Secret, Token, Secret.
You can get these from your twitter apps page: https://apps.twitter.com/

4. Confirm your topic was created using kafka command line: `kafka-topics --zookeeper localhost:2181 --list`

5. Read from the Kafka topic: `sudo bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic twitter-topic --from-beginning`