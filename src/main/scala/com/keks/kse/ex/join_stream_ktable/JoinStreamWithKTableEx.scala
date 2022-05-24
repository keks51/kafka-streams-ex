package com.keks.kse.ex.join_stream_ktable

import io.circe.generic.auto._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.util.Properties


object JoinStreamWithKTableEx {

  object Domain {
    type UserId = String
    type OrderId = String

    case class Order(orderId: OrderId, text: String)
    case class User(userId: UserId, name: String, age: Int)
  }

  object Topics {
    val ORDER_TOPIC = "order_topic_jswkt"
    val USERS_KTABLE_TOPIC = "users_ktable_topic_jswkt"
  }

  import Domain._

  def buildTopology: Topology = {
    import com.keks.kse.ex.utils.SerdeUtils.serde
    val builder = new StreamsBuilder()

    val orderStream: KStream[UserId, Order] = builder.stream(Topics.ORDER_TOPIC)
    val userTable: KTable[UserId, User] = builder.table(Topics.USERS_KTABLE_TOPIC)

    val joined: KStream[UserId, String] = orderStream.join(userTable) { case(Order(orderId, text), User(userId, name, age)) =>
      s"UserId: '$userId'. OrderId: '$orderId'. Ordered '$text'. user name: '$name', age: '$age'"
    }
    joined.foreach((_, e) => println(e))
    builder.build()
  }

  def buildStreamApp(topology: Topology): KafkaStreams = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "autocommit-app")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000)

    new KafkaStreams(topology, props)
  }


  def main(args: Array[String]): Unit = {

    val builder = buildTopology
    val app = buildStreamApp(builder)
    app.start()

    Thread.sleep(5000)
    app.close()
  }

}