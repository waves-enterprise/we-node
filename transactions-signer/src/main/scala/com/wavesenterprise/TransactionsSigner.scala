package com.wavesenterprise

import java.io.{File, FileInputStream}
import com.typesafe.scalalogging.LazyLogging
import com.wavesenterprise.transaction._
import com.wavesenterprise.utils.Time
import com.wavesenterprise.wallet.Wallet
import play.api.libs.json.{JsArray, JsObject, JsValue, Json}
import cats.implicits._
import com.google.common.io.Closeables
import com.wavesenterprise.api.http.JsonTransactionParser

object TransactionsSigner extends LazyLogging {
  private val jsonTransactionParser = new JsonTransactionParser()

  private val time: Time = new Time {
    def correctedTime(): Long = System.currentTimeMillis()
    def getTimestamp(): Long  = System.currentTimeMillis()
  }

  def readAndSign(file: File, wallet: Wallet): List[BroadcastData] = {
    val inputStream = new FileInputStream(file)
    val rootJson =
      try {
        Json.parse(inputStream).as[JsArray]
      } catch {
        case ex: Exception =>
          logger.error(s"Failed to read json from '${file.getAbsolutePath}' file, reason: '${ex.getMessage}'")
          sys.exit()
      } finally {
        Closeables.close(inputStream, false)
      }

    val signResult = rootJson.value.map { txJs =>
      val certificates = (txJs \ "certificates").toOption.map(_.as[List[String]]).getOrElse(List.empty[String])
      signTx(txJs, wallet).map(signedTx => BroadcastData(signedTx, certificates))
    }

    val (parseErrors, signedTransactions) = signResult.toList.separate
    logger.info(
      s"Total transactions in file: '${signResult.length}', failed to sign: '${parseErrors.length}', success: '${signedTransactions.length}'")
    if (parseErrors.nonEmpty) {
      logger.warn(s"Sign errors:\n======\n${parseErrors.mkString("\n")}\n======")
    }
    signedTransactions
  }

  private def signTx(jsValue: JsValue, wallet: Wallet): Either[String, ProvenTransaction] =
    jsonTransactionParser
      .signTransaction(jsValue.as[JsObject], wallet, time, checkCerts = false)
      .leftMap(validationErr => validationErr.toString)

  case class BroadcastData(tx: ProvenTransaction, certificates: List[String])

}
