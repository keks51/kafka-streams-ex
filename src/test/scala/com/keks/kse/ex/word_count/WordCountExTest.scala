package com.keks.kse.ex.word_count

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.state.{KeyValueIterator, KeyValueStore, ValueAndTimestamp}
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TopologyTestDriver}
import utils.TestBase

import java.util.Properties

class WordCountExTest extends TestBase {

  "fdfd" should "fdfd" in withTempDir { dir =>
    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "WordCountApp")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config")
      // Use a temporary directory for storing state, which will be automatically removed after the test.
      p.put(StreamsConfig.STATE_DIR_CONFIG, s"$dir/stage_dir")
      p
    }

    val testDriver = new TopologyTestDriver(WordCountEx.buildTopology, streamsConfiguration)

    val orderTopic: TestInputTopic[String, String] = createTopic[String, String](WordCountEx.Topics.TEXT_LINES_TOPIC, testDriver)
    orderTopic.pipeInput("user_1", "i bought the car yesterday")
    orderTopic.pipeInput("user_2", "I want to go to the shop")
    orderTopic.pipeInput("user_3", "What a great weather is today")


    val keyValueStore: KeyValueStore[Bytes, String] = testDriver.getKeyValueStore(WordCountEx.KeyValueStore.MY_STORE)
    val keyValueStoreIter: KeyValueIterator[Bytes, String] = keyValueStore.all()
    while (keyValueStoreIter.hasNext) {
      println(keyValueStoreIter.next())
    }

    println("")

    val timestampedKeyValueStore: KeyValueStore[Integer, ValueAndTimestamp[String]] = testDriver.getTimestampedKeyValueStore(WordCountEx.KeyValueStore.MY_STORE)
    val timestampedKeyValueStoreIter: KeyValueIterator[Integer, ValueAndTimestamp[String]] = timestampedKeyValueStore.all()
    while (timestampedKeyValueStoreIter.hasNext) {
      println(timestampedKeyValueStoreIter.next())
    }

  }

}
