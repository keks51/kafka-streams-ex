package com.keks.kse.ex.custom_store


import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TopologyTestDriver}
import utils.TestBase

import java.util.Properties


class CustomTransformerExTest extends TestBase {

  "fdfd" should "fdfd" in withTempDir { dir =>
    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "CustomTransformerEx")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config")
      // Use a temporary directory for storing state, which will be automatically removed after the test.
      p.put(StreamsConfig.STATE_DIR_CONFIG, s"$dir/stage_dir")
      p
    }
    val testDriver = new TopologyTestDriver(CustomTransformerEx.buildTopology, streamsConfiguration)

    val orderTopic: TestInputTopic[String, String] = createTopic[String, String](CustomTransformerEx.Topics.CUSTOM_TRANSFORMER_TOPIC, testDriver)
    orderTopic.pipeInput("user_1", "1")
    orderTopic.pipeInput("user_1", "2")
    orderTopic.pipeInput("user_1", "3")
  }
}
