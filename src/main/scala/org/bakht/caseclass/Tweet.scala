package org.bakht.caseclass

case class Tweet (
                 text: String,
                 user_id: Long,
                 id: Long,
                 created_at: String,
                 lang: String,
                 place: String,
                 hashtags: Option[List[String]]
                 )
