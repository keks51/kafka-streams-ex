package com.keks.kse.ex.word_count

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable, Materialized}
import org.apache.kafka.streams.scala.serialization.Serdes.longSerde
import org.apache.kafka.streams.state.KeyValueStore


object WordCountEx {

  object Domain {
    type ID = String
  }

  object Topics {
    val TEXT_LINES_TOPIC = "text_lines_topic"
  }

  object KeyValueStore {
    val MY_STORE = "my_store"
  }

  import Domain._

  def buildTopology: Topology = {
    import com.keks.kse.ex.utils.SerdeUtils.serde
    val builder = new StreamsBuilder()

    val stream = builder.stream[ID, String](Topics.TEXT_LINES_TOPIC)

    val wordsStream: KStream[ID, String] = stream.flatMapValues(e => e.toLowerCase.split(" "))

    val groupedStream: KGroupedStream[ID, String] = wordsStream.groupBy((_, v) => v)

    val mater = Materialized.as[ID, Long, KeyValueStore[Bytes, Array[Byte]]](KeyValueStore.MY_STORE)

    val res: KTable[ID, Long] = groupedStream.count()(mater)

    res.toStream.foreach((k, v) => println(s"K: '$k'. V: '$v'"))

    builder.build()
  }

  def main(args: Array[String]): Unit = {

  }

}
