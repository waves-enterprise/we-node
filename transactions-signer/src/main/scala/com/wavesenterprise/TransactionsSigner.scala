package com.wavesenterprise

import java.io.{File, FileInputStream}

import com.typesafe.scalalogging.LazyLogging
import com.wavesenterprise.transaction._
import com.wavesenterprise.utils.Time
import com.wavesenterprise.wallet.Wallet
import play.api.libs.json.{JsArray, JsObject, JsValue, Json}
import cats.implicits._
import com.google.common.io.Closeables

object TransactionsSigner extends LazyLogging {
  private val time: Time = new Time {
    def correctedTime(): Long = System.currentTimeMillis()
    def getTimestamp(): Long  = System.currentTimeMillis()
  }

  def readAndSign(file: File, wallet: Wallet): List[ProvenTransaction] = {
    val inputStream = new FileInputStream(file)
    val rootJson = try {
      Json.parse(inputStream).as[JsArray]
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to read json from '${file.getAbsolutePath}' file, reason: '${ex.getMessage}'")
        sys.exit()
    } finally {
      Closeables.close(inputStream, false)
    }

    val signResult = rootJson.value.map(txJs => signTransactions(txJs, wallet))

    val (parseErrors, signedTransactions) = signResult.toList.separate
    logger.info(
      s"Total transactions in file: '${signResult.length}', failed to sign: '${parseErrors.length}', success: '${signedTransactions.length}'")
    if (parseErrors.nonEmpty) {
      logger.warn(s"Sign errors:\n======\n${parseErrors.mkString("\n")}\n======")
    }
    signedTransactions
  }

  private def signTransactions(jsValue: JsValue, wallet: Wallet): Either[String, ProvenTransaction] = {
    api.http
      .signTransaction(jsValue.as[JsObject], wallet, time)
      .leftMap(validationErr => validationErr.toString)
  }
}
