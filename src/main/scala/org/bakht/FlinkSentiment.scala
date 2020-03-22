package org.bakht
import java.io.FileInputStream
import java.util.Properties

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.bakht.caseclass.Tweet
import org.bakht.mappers.TweetMapper
import org.bakht.utils.TwitterEndpointFilter
import com.google.auth.oauth2.ServiceAccountCredentials
import org.bakht.mappers.SentimentAnalysis

import scala.collection.JavaConverters._

// Twitter Props required
//    CONSUMER_KEY
//    CONSUMER_SECRET
//    TOKEN
//    TOKEN_SECRET

object FlinkSentiment extends App {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kdsProps: Map[String,Properties]  = KinesisAnalyticsRuntime.getApplicationProperties().asScala.toMap
    val twitterProps: Properties = kdsProps("FlinkApplicationProperties")
    twitterProps.setProperty(TwitterSource.CONSUMER_KEY,twitterProps.get("consumer").toString)
    twitterProps.setProperty(TwitterSource.CONSUMER_SECRET,twitterProps.get("consumer-secret").toString)
    twitterProps.setProperty(TwitterSource.TOKEN,twitterProps.get("token").toString)
    twitterProps.setProperty(TwitterSource.TOKEN_SECRET,twitterProps.get("token-secret").toString)


    val gcpCredJson: String = kdsProps("GCPProperties").getProperty("CREDS")
    val twitterSource: TwitterSource = new TwitterSource(twitterProps)
    twitterSource.setCustomEndpointInitializer(new TwitterEndpointFilter())

    val gcpCred = ServiceAccountCredentials.fromStream(new FileInputStream(gcpCredJson))
    val tweets: DataStream[Tweet] = env
      .addSource(twitterSource)
        .flatMap(new TweetMapper)

    tweets
      .flatMap(new SentimentAnalysis(gcpCred))
        .print("Sentiment")


    tweets.print
    env.execute("flink-sentiment-app")


}


