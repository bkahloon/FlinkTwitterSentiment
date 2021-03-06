package org.bakht.mappers

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector
import org.bakht.caseclass.Tweet
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization.write

import scala.collection.mutable.ListBuffer

class TweetMapper extends RichFlatMapFunction[String,Tweet] {
  implicit lazy val formats = DefaultFormats

  override def flatMap(in: String, out: Collector[Tweet]): Unit = {
    try {
      val json = parse(in).extract[Map[String, Any]]
      if (json contains "id")
        out.collect(reformatTweet(json))
    }catch{
      case ex: org.json4s.ParserUtil.ParseException =>
    }
  }

  
  def reformatTweet(x: Map[String,Any]): Tweet = {

    val place: Option[String] = if (x contains "place")
      Some("test")//Some(x("place").asInstanceOf[Map[String,Any]]("country_code").toString)
     else None
    val processed_doc = scala.collection.mutable.Map(
      "id" -> x("id"),
      "lang" -> x("lang"),
      "place" ->  place,
      "user_id" -> x("user").asInstanceOf[Map[String,Any]]("id"),
      "created_at" -> x("created_at")
    )
    if (x contains ("extended_tweet"))
      processed_doc("text") = x("extended_tweet").asInstanceOf[Map[String,Any]]("full_text")
    else if (x contains ("full_text"))
      processed_doc("text") = x("full_text")
    else
    processed_doc("text") = x("text")

    if ((x contains ("entities") )
      && (x("entities").asInstanceOf[Map[String,Any]] contains "hashtags")){
      val hashtagList: List[Map[String,Any]] = x("entities").asInstanceOf[Map[String,Any]]("hashtags").asInstanceOf[List[Map[String,Any]]]
      var hashtags: ListBuffer[String] = new ListBuffer()
      hashtagList.foreach(entry => {
        hashtags += entry("text").toString
      })
      processed_doc("hashtags") = Some(hashtags.toList)
    }
    else
    processed_doc("hashtags") = None
    parse(write(processed_doc)).extract[Tweet]

  }
}