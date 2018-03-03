package com.wavesplatform.lang.traits
import scala.util.Try

trait Base58 {
  protected val Base58Chars: String

  protected def base58Encode(input: Array[Byte]): String

  protected def base58Decode(input: String): Try[Array[Byte]]
}