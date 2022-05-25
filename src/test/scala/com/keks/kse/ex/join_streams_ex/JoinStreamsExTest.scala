package com.keks.kse.ex.join_streams_ex

import com.keks.kse.ex.join_streams_ex.JoinStreamsEx.Domain.{Order, Paid, UserId}
import io.circe.generic.auto._
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TopologyTestDriver}
import utils.{KafkaUtils, TestBase}

import java.util.Properties;


class JoinStreamsExTest extends TestBase {

  "fdfd" should "fdf" in withTempDir { dir =>
    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "JoinStreams")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config")
      // Use a temporary directory for storing state, which will be automatically removed after the test.
      p.put(StreamsConfig.STATE_DIR_CONFIG, s"$dir/stage_dir")
      p
    }


    val testDriver = new TopologyTestDriver(JoinStreamsEx.buildTopology, streamsConfiguration)

    val orderTopic: TestInputTopic[UserId, Order] = createTopic[UserId, Order](JoinStreamsEx.Topics.ORDER_TOPIC, testDriver)
    orderTopic.pipeInput("user_1", JoinStreamsEx.Domain.Order("1", "av"))
    orderTopic.pipeInput("user_2", JoinStreamsEx.Domain.Order("2", "av"))
    orderTopic.pipeInput("user_3", JoinStreamsEx.Domain.Order("3", "av"))

    val paidTopic: TestInputTopic[UserId, Paid] = createTopic[UserId, Paid](JoinStreamsEx.Topics.PAID_TOPIC, testDriver)
    paidTopic.pipeInput("user_1", JoinStreamsEx.Domain.Paid("1", "av"))
    paidTopic.pipeInput("user_2", JoinStreamsEx.Domain.Paid("2", "av"))
    paidTopic.pipeInput("user_3", JoinStreamsEx.Domain.Paid("3", "av"))

    Thread.sleep(5000)
    paidTopic.pipeInput("user_3", JoinStreamsEx.Domain.Paid("4", "ab"))

    Thread.sleep(5000)

  }



}
