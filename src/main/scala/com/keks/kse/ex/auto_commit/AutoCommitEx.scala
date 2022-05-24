package com.keks.kse.ex.auto_commit

import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.kafka.streams.scala.ImplicitConversions._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.util.Properties



// In kafka streams option 'enable.auto.commit' is true by default.
// if streams reads message from topic, __consumer_offsets topic adds new offset after 'commit.interval.ms' interval.
// If stream is closed via 'app.close()' before end of 'commit.interval.ms' interval then __consumer_offsets topic adds new offset too.
// if app is closed like 'kill app' then then __consumer_offsets topic DOESN'T add new offset.
// For exactly one semantic 'commit.interval.ms' is 100ms.
// You can manually commit offsets by creating custom processor.
object AutoCommitEx {

  // Orders topic

  object Domain {
    type OrderId = String

    case class Order(orderId: OrderId, text: String)
  }


  object Topics {
    val ORDER_TOPIC = "autocommit_topic"
  }

  import Domain._

  implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
    val ser = (a: A) => a.asJson.noSpaces.getBytes()
    val des = (bytes: Array[Byte]) => {
      val str = new String(bytes)
      decode[A](str).toOption
    }
    Serdes.fromFn[A](ser, des)
  }


  def main(args: Array[String]): Unit = {

    val builder = new StreamsBuilder()

    val usersOrdersStream: KStream[OrderId, Order] = builder.stream(Topics.ORDER_TOPIC)

    usersOrdersStream
      .foreach { case (id, Order(orderId, text)) => println(s"MessageId: $id, Order(ID:$orderId, Text:$text)") }

    val topology = builder.build()

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "autocommit-app")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000)

    val app = new KafkaStreams(topology, props)

    app.start()

    Thread.sleep(5000)
//    println("closing")
    app.close()
  }

}
