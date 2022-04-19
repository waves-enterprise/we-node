package com.wavesenterprise.crypto

import java.security._
import java.security.spec.X509EncodedKeySpec

import scala.util.Try

object Rsa {

  private val KeyPairAlgorithm   = "RSA"
  private val SignatureAlgorithm = "SHA256withRSA"

  def verify(data: Array[Byte], signature: Array[Byte], publicKey: java.security.PublicKey): Boolean = {
    val publicSignature = Signature.getInstance(SignatureAlgorithm)

    Try {
      publicSignature.initVerify(publicKey)
      publicSignature.update(data)
      publicSignature.verify(signature)
    }.getOrElse(false)
  }

  def publicKey(bytes: Array[Byte]): java.security.PublicKey = {
    KeyFactory.getInstance(KeyPairAlgorithm).generatePublic(new X509EncodedKeySpec(bytes))
  }
}
