package utils

import com.keks.kse.ex.utils.SerdeUtils
import io.circe.{Decoder, Encoder}
import org.apache.kafka.streams.{TestInputTopic, TopologyTestDriver}

trait KafkaUtils {

  def createTopic[K >: Null : Decoder : Encoder, V >: Null : Decoder : Encoder](topicName: String, driver: TopologyTestDriver): TestInputTopic[K, V] = {
    driver
      .createInputTopic(
        topicName,
        SerdeUtils.serde[K].serializer(),
        SerdeUtils.serde[V].serializer())
  }

}
