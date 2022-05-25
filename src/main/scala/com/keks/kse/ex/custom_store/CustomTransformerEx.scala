package com.keks.kse.ex.custom_store

import com.keks.kse.ex.custom_store.CustomTransformerEx.Domain.Amount
import io.circe.generic.auto._
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}
import org.apache.kafka.streams.{KeyValue, Topology}


object CustomTransformerEx {

  object Domain {
    type ID = String

    case class Amount(value: Int)
  }

  object Topics {
    val CUSTOM_TRANSFORMER_TOPIC = "custom_store_topic"
  }

  object KeyValueStore {
    val MY_STORE = "my_store"
  }

  import Domain._

  def buildTopology: Topology = {
    import com.keks.kse.ex.utils.SerdeUtils.serde
    val builder = new StreamsBuilder()

    val myStore: StoreBuilder[KeyValueStore[ID, Amount]] = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore(KeyValueStore.MY_STORE),
      Serdes.String(),
      serde[Amount]
    )
    builder.addStateStore(myStore)

    val stream = builder.stream[ID, String](Topics.CUSTOM_TRANSFORMER_TOPIC)
    val res: KStream[String, Amount] = stream.transform(new MyTransformerSupplier(KeyValueStore.MY_STORE), KeyValueStore.MY_STORE)

    res.foreach((k, v) => println(s"K: '$k', V1: '$v'"))
    builder.build()
  }

  def main(args: Array[String]): Unit = {

  }

}

class MyTransformerSupplier(storeName: String) extends TransformerSupplier[String, String, KeyValue[String, Amount]] {

  override def get(): Transformer[String, String, KeyValue[String, Amount]] = new MyTransformer(storeName)

}

class MyTransformer(storeName: String) extends Transformer[String, String, KeyValue[String, Amount]] {
  var store: KeyValueStore[String, Amount] = _

  override def init(context: ProcessorContext): Unit = {
    store = context.getStateStore(storeName)
  }

  override def transform(key: String, value: String): KeyValue[String, Amount] = {
    val valueInt = value.toInt

    val res: KeyValue[String, Amount] = Option(store.get(key)) match {
      case Some(previousVal) =>

        store.put(key, Amount(previousVal.value * valueInt))
        new KeyValue(key, Amount(previousVal.value * valueInt))
      case None =>
        store.put(key, Amount(valueInt))
        new KeyValue(key, Amount(valueInt))
    }

    res

  }

  override def close(): Unit = {

  }
}