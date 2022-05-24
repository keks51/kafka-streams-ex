package com.keks.kse.ex.utils

import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder}
import io.circe.parser.decode
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.serialization.Serdes


object SerdeUtils {

  implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
    val ser = (a: A) => a.asJson.noSpaces.getBytes()
    val des = (bytes: Array[Byte]) => {
      val str = new String(bytes)
      decode[A](str).toOption
    }
    Serdes.fromFn[A](ser, des)
  }
}
