package com.keks.kse.ex

import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties

object KafkaStreams {

  object Domain {
    type UserId = String
    type Profile = String
    type Product = String
    type OrderId = String
    type Status = String

    case class Order(orderId: OrderId, userId: UserId, products: List[Product], amount: Double)

    case class Discount(profile: Profile, amount: Double)

    case class Payment(orderId: OrderId, status: Status)
  }

  object Topics {
    val ORDERS_BY_USER = "orders-by-user"
    val DISCOUNT_PROFILE_BY_USER = "discount-profile-by-user"
    val DISCOUNTS = "discounts"
    val ORDERS = "orders"
    val PAYMENTS = "payments"
    val PAID_ORDERS = "paid-orders"
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


    val usersOrdersStream: KStream[UserId, Order] = builder.stream(Topics.ORDERS_BY_USER)

    val userProfilesTable: KTable[UserId, Profile] = builder.table(Topics.DISCOUNT_PROFILE_BY_USER)

    val discountProfileGTable: GlobalKTable[Profile, Discount] = builder.globalTable(Topics.DISCOUNTS)


    val expensiveOrders: KStream[UserId, Order] = usersOrdersStream.filter { (userId, order) =>
      order.amount > 1000
    }

    val listOfProducts: KStream[UserId, List[Product]] = usersOrdersStream.mapValues { order =>
      order.products
    }

    val productsStream: KStream[UserId, Product] = usersOrdersStream.flatMapValues(_.products)

    val ordersWithUserProfiles: KStream[UserId, (Order, Profile)] = usersOrdersStream
      .join(userProfilesTable) { (order, profiles) => (order, profiles) }

    val discountOrdersStream: KStream[UserId, Order] = ordersWithUserProfiles.join(discountProfileGTable)(
      { case (userId, (order, profile)) => profile },
      { case ((order, profile), discount) => order.copy(amount = order.amount - discount.amount) }
    )

    val ordersStream: KStream[OrderId, Order] = discountOrdersStream.selectKey((userId, order) => order.orderId)
    val paymentsStream: KStream[OrderId, Payment] = builder.stream[OrderId, Payment](Topics.PAYMENTS)

    val joinWindow: JoinWindows = JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES))
    val joinCondition: (Order, Payment) => Option[Order] = (order: Order, payment: Payment) =>
      Option(if (payment.status == "PAID") order else null)

    val ordersPaid: KStream[OrderId, Order] = ordersStream
      .join(paymentsStream)(joiner = joinCondition, windows = joinWindow)
      .flatMapValues(e => e)

    ordersPaid.to(Topics.PAID_ORDERS)

    val topology = builder.build()

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

    //    println(topology.describe())

    val app = new KafkaStreams(topology, props)
    app.start()

  }

}
