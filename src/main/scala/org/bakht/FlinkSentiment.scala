package org.bakht
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.bakht.caseclass.{Tweet, TweetSentiment}
import org.bakht.mappers.TweetMapper
import org.bakht.utils.TwitterEndpointFilter
import com.google.auth.oauth2.ServiceAccountCredentials
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.bakht.mappers.SentimentAnalysis
import org.bakht.serde.KafkaSerializer

import scala.collection.JavaConverters._
import scala.io.Source

// Twitter Props required
//    CONSUMER_KEY
//    CONSUMER_SECRET
//    TOKEN
//    TOKEN_SECRET

// GCP service account json required

// Optional twitter key word filters

object FlinkSentiment extends App {

    val logger = LoggerFactory.getLogger(this.getClass)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val twitterProps: Properties = new Properties()
    twitterProps.setProperty(TwitterSource.CONSUMER_KEY,scala.util.Properties.envOrElse("CONSUMER_KEY", "" ))
    twitterProps.setProperty(TwitterSource.CONSUMER_SECRET,scala.util.Properties.envOrElse("CONSUMER_SECRET", "" ))
    twitterProps.setProperty(TwitterSource.TOKEN,scala.util.Properties.envOrElse("TOKEN", "" ))
    twitterProps.setProperty(TwitterSource.TOKEN_SECRET,scala.util.Properties.envOrElse("TOKEN_SECRET", ""))

    val gcpCredJson: String = scala.util.Properties.envOrElse("GOOGLE_CRED", "")
    val twitterSource: TwitterSource = new TwitterSource(twitterProps)
    var twitterKeyWords: List[String] = List()
    if(!args.isEmpty){
        twitterKeyWords = args.toList
    }
    if (twitterKeyWords.nonEmpty) {
      twitterSource.setCustomEndpointInitializer(new TwitterEndpointFilter(twitterKeyWords))
    } else twitterSource.setCustomEndpointInitializer(new TwitterEndpointFilter())

    val gcpCred = ServiceAccountCredentials.fromStream(getClass.getResourceAsStream(gcpCredJson))
    val tweets: DataStream[Tweet] = env
      .addSource(twitterSource)
        .flatMap(new TweetMapper)

    val producerProp: Properties = new Properties()
    producerProp.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    val kafkaProducer = new FlinkKafkaProducer[TweetSentiment](
        "my-topic",
        new KafkaSerializer[TweetSentiment],
        producerProp
    )

//    val tweetSentiment = tweets
//      .flatMap(new SentimentAnalysis(gcpCred))

    tweets
      .print()
       /// .addSink(kafkaProducer)



    env.execute("flink-sentiment-app")


}


