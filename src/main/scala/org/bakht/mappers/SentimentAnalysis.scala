package org.bakht.mappers

import com.google.api.gax.core.FixedCredentialsProvider
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.language.v1.{AnalyzeEntitySentimentRequest, Document, EncodingType, LanguageServiceClient, LanguageServiceSettings}
import com.google.cloud.language.v1.Document.Type
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.bakht.caseclass.{Tweet, TweetSentiment}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
// Imports the Google Cloud client library


class SentimentAnalysis(gcpCred: ServiceAccountCredentials) extends RichFlatMapFunction[Tweet,TweetSentiment]  {

  private var language: LanguageServiceClient = _

  override def open(parameters: Configuration): Unit = {
    val fixedCred = FixedCredentialsProvider.create(gcpCred)
    val languageServiceSettings: LanguageServiceSettings = LanguageServiceSettings
      .newBuilder()
      .setCredentialsProvider(fixedCred)
      .build()
    language =  LanguageServiceClient.create(languageServiceSettings)
  }

  override def close(): Unit = {
    if (language != null) language.close()
  }

  override def flatMap(t: Tweet, out: Collector[TweetSentiment]): Unit = {
    val entityPredictions: Option[List[TweetSentiment]] = predict(t)
    entityPredictions match {
      case Some(validList) => validList.foreach(x =>  out.collect(x))
      case None =>
    }
  }

  def predict(tweet: Tweet): Option[List[TweetSentiment]] = {
    val data = tweet.text
    val emptyCase = Some("")

    Some(data) match {
      case `emptyCase` => None
      case Some(v) => Some(entitySentimentFile(data))
    }

  }

  def entitySentimentFile(text: String): List[TweetSentiment] = { // [START language_entity_sentiment_gcs]
    // Instantiate the Language client com.google.cloud.language.v1.LanguageServiceClient
    val entityList: ListBuffer[TweetSentiment] = ListBuffer()
    val doc = Document.newBuilder.setContent(text).setType(Type.PLAIN_TEXT).build
    val request: AnalyzeEntitySentimentRequest = AnalyzeEntitySentimentRequest
      .newBuilder.setDocument(doc).setEncodingType(EncodingType.UTF16).build
    // Detect entity sentiments in the given text
    val response = language.analyzeEntitySentiment(request)
    for (entity <- response.getEntitiesList) {
      entityList += TweetSentiment(entity.getName,entity.getType.toString,entity.getSentiment.getScore)
    }
    entityList.toList
  }
}