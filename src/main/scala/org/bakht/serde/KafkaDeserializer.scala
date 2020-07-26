package org.bakht.serde

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.bakht.caseclass.Tweet

class KafkaDeserializer extends DeserializationSchema[Tweet] {
  lazy val obj: ObjectMapper = new ObjectMapper()

  override def deserialize(message: Array[Byte]): Tweet = {
    obj.readValue(message,classOf[Tweet])
  }
  override def isEndOfStream(nextElement: Tweet): Boolean = false

  override def getProducedType: TypeInformation[Tweet] = TypeInformation.of(classOf[Tweet])
}