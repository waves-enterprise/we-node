package com.wavesenterprise.docker

import java.security.{KeyPair, KeyPairGenerator, SecureRandom}
import java.time.Clock

import cats.implicits._
import pdi.jwt.{Jwt, JwtAlgorithm, JwtClaim}
import play.api.libs.json.{JsValue, Json, Reads}

import scala.concurrent.duration.FiniteDuration

trait ClaimContent {

  def toJson: JsValue
}

class ContractAuthTokenService {

  implicit val clock: Clock = Clock.systemUTC()

  private[this] val keyPair: KeyPair = {
    val keyGen = KeyPairGenerator.getInstance("RSA")
    val random = SecureRandom.getInstance("SHA1PRNG")
    keyGen.initialize(2048, random)
    keyGen.generateKeyPair()
  }

  def create(claimContent: ClaimContent, expiresIn: FiniteDuration): String = {
    Jwt.encode(JwtClaim(claimContent.toJson.toString()).issuedNow.expiresIn(expiresIn.toSeconds), keyPair.getPrivate, JwtAlgorithm.RS256)
  }

  def validate[T <: ClaimContent](token: String)(implicit reads: Reads[T]): Either[Throwable, T] =
    for {
      claim <- Either.fromTry(Jwt.decode(token, keyPair.getPublic, Seq(JwtAlgorithm.RS256)))
      parsed = Json.parse(claim.content)
      content <- reads.reads(parsed).asEither.leftMap(errors => new RuntimeException(s"Claim content reading failed: '$errors''"))
    } yield content
}
