package org.bakht.serde

import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
import org.json4s.DefaultFormats
import org.json4s.native.Serialization.write

class KafkaSerializer[A](keyExtractor:(A) => String = null ) extends KeyedSerializationSchema[A] {

  implicit lazy val formats = DefaultFormats

  override def serializeKey(t: A): Array[Byte] = {
    if (keyExtractor == null) null
    else keyExtractor(t).getBytes()
  }

  override def serializeValue(t: A): Array[Byte] = {

    var byteArray: Array[Byte] = null
    if (t.isInstanceOf[String]){
       t.toString.getBytes()
    }else{
      write(t).getBytes()
    }

  }

  override def getTargetTopic(t: A): String = null

}