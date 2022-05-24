package com.keks.kse.test

import io.circe.generic.auto._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.util.Properties


object HighLoadAppEx {

  object Domain {
    type ID = String

    case class User(id: ID, name: String, age: Int)
    case class Event(text: String)

    case class UserEvent(userId: ID, eventText: String, name: String, age: Int)
  }


  object Topics {
    val USER_K_TABLE_TOPIC = "user_k_table_topic"
    val EVENT_TOPIC = "event_topic"
    val USER_EVENT_TOPIC = "event_topic"
  }

  def buildTopology: Topology = {
    import Domain._
    import Topics._
    import com.keks.kse.ex.utils.SerdeUtils.serde
    val builder = new StreamsBuilder()

    val eventStream: KStream[ID, Event] = builder.stream[ID, Event](EVENT_TOPIC)
    val userTable: KTable[ID, User] = builder.table[ID, User](USER_K_TABLE_TOPIC)

    val joined: KStream[ID, UserEvent] = eventStream.join(userTable) { case (Event(text), User(id, name, age)) =>
      UserEvent(id, text, name, age)
    }
    joined.to(USER_EVENT_TOPIC)

    builder.build()
  }

  def buildStreamApp(topology: Topology): KafkaStreams = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "HighLoadAppEx")
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
