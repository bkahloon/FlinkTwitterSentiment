package org.bakht.utils

import com.twitter.hbc.core.endpoint.{StatusesFilterEndpoint, StreamingEndpoint}
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint

import scala.collection.JavaConverters._

// val chicago = new Location(new Location.Coordinate(-86.0, 41.0), new Location.Coordinate(-87.0, 42.0))

//////////////////////////////////////////////////////
// Create an Endpoint to Track our terms
class TwitterEndpointFilter(hashtags:List[String]=List("covid"))
  extends TwitterSource.EndpointInitializer with Serializable {
  @Override
  def createEndpoint(): StreamingEndpoint = {
    val endpoint = new StatusesFilterEndpoint()
    //endpoint.locations(List(chicago).asJava)
    endpoint.trackTerms(hashtags.asJava)
    endpoint
  }
}
