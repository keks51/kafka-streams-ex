package com.keks.kse.ex.join_stream_ktable

import io.circe.generic.auto._
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TopologyTestDriver}
import utils.{KafkaUtils, TestBase}

import java.util.Properties


class JoinStreamWithKTableExTest extends TestBase with KafkaUtils {

  "fdfd" should "fdfd" in withTempDir { dir =>
    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "JoinStreamWithKTable")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config")
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
      // Use a temporary directory for storing state, which will be automatically removed after the test.
      p.put(StreamsConfig.STATE_DIR_CONFIG, s"$dir/stage_dir")
      p
    }

    val testDriver = new TopologyTestDriver(JoinStreamWithKTableEx.buildTopology, streamsConfiguration)
    import JoinStreamWithKTableEx.Domain._
    import JoinStreamWithKTableEx.Topics._

    val userTopic: TestInputTopic[UserId, User] = createTopic[UserId, User](USERS_KTABLE_TOPIC, testDriver)
    userTopic.pipeInput("user_1", User("1", "alex", 25))
    userTopic.pipeInput("user_2", User("2", "den", 30))
    userTopic.pipeInput("user_3", User("3", "mike", 41))


    val orderTopic: TestInputTopic[UserId, Order] = createTopic[UserId, Order](ORDER_TOPIC, testDriver)

    orderTopic.pipeInput("user_1", Order("1", "av"))
    orderTopic.pipeInput("user_2", Order("2", "av"))
    orderTopic.pipeInput("user_3", Order("3", "av"))

    Thread.sleep(5000)
  }

}
