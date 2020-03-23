package org.bakht.caseclass
import org.json4s.DefaultFormats
import org.json4s.native.Serialization.write

case class TweetSentiment (
                   name: String,
                   entityType: String,
                   score: Float,
                   text: String
                 )
object TweetSentiment {
  implicit lazy val formats = DefaultFormats
  override def toString: String = write(this)
}
