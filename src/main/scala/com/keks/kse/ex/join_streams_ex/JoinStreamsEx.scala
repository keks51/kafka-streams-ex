package com.keks.kse.ex.join_streams_ex

import io.circe.generic.auto._
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.time.Duration
import java.util.Properties


object JoinStreamsEx {

  object Domain {
    type UserId = String
    type OrderId = String
    type PaidId = String

    case class Order(orderId: OrderId, text: String)
    case class Paid(paidId: PaidId, text: String)
  }

  object Topics {
    val ORDER_TOPIC = "order_topic_js"
    val PAID_TOPIC = "paid_topic_js"
  }

  import Domain._

  def buildTopology: Topology = {
    import com.keks.kse.ex.utils.SerdeUtils.serde
    val builder = new StreamsBuilder()

    val ordersStream: KStream[UserId, Order] = builder.stream(Topics.ORDER_TOPIC)
    val paidStream: KStream[UserId, Paid] = builder.stream(Topics.PAID_TOPIC)

    val joinCondition: (Order, Paid) => String = (order: Order, paid: Paid) => order.orderId + " : " + paid.paidId
    val window: JoinWindows = JoinWindows.of(Duration.ofSeconds(10))
    ordersStream
      .join(paidStream)(joinCondition, window)
      .foreach((k, v) => println(s"Key: $k, Value: $v"))
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