package com.wavesenterprise

import cats.implicits.catsSyntaxEither
import ch.qos.logback.classic.{Level, LoggerContext}
import com.google.common.io.Closeables
import com.typesafe.scalalogging.LazyLogging
import com.wavesenterprise.account.AddressScheme
import com.wavesenterprise.crypto.CryptoInitializer
import com.wavesenterprise.settings.CryptoSettings.cryptoSettingsFromString
import com.wavesenterprise.settings.{CryptoSettings, WalletSettings}
import com.wavesenterprise.wallet.Wallet
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsArray, JsValue, Json}
import scopt.OptionParser

import java.io.{BufferedWriter, File, FileWriter}

object TxSignerApplication extends LazyLogging {

  private val parser = new OptionParser[TxSignerSettings]("transaction signer") {
    head("TransactionsSigner - WE json transactions signer")
    note(
      """To get signed transaction with pki certificates, add it to input transaction structure '{... "certificates": [<base64EncodedCerts>] ...}'""")
    opt[String]('i', "input-path").valueName("<path>").text("path to input transactions json").action { (value, settings) =>
      settings.copy(inputTransactionsFile = new File(value))
    }
    opt[String]('k', "keystore-path").valueName("<path>").text("path to keystore").action { (value, settings) =>
      settings.copy(keystoreFile = new File(value))
    }
    opt[String]('p', "keystore-password").valueName("<password>").text("password to keystore").action { (value, settings) =>
      settings.copy(keystorePassword = value)
    }
    opt[String]('c', "crypto-type").valueName("<crypto type>").text("crypto type").action { (value, settings) =>
      settings.copy(cryptoType = value)
    }
    opt[Char]('b', "chain-char").valueName("<char>").text("chain character").action { (value, settings) =>
      settings.copy(chainByte = value.toByte)
    }
    opt[String]('o', "output-path").valueName("<path>").text("path to output transactions json").action { (value, settings) =>
      settings.copy(outputTransactionsFile = new File(value))
    }
    opt[String]('l', "logging-level").valueName("<level>").text("logging level").action { (value, settings) =>
      settings.copy(loggingLevel = Level.valueOf(value.toUpperCase()))
    }

    help("help").text("display this help message")
  }

  def main(args: Array[String]): Unit = {
    val default = TxSignerSettings.empty
    parser.parse(args, default) match {
      case None => logger.error(s"Signer can't start - failed to parse config")
      case Some(resultCfg) =>
        resultCfg.validate() match {
          case Left(errorMessage) => logger.error(s"Config validation error: '$errorMessage'"); sys.exit(1)
          case Right(_)           => startSigner(resultCfg)
        }
    }
  }

  private def startSigner(settings: TxSignerSettings): Unit = {
    setLoggingLevel(settings.loggingLevel)
    logger.info(
      s"Start transactions signer\n\tinput path: '${settings.inputTransactionsFile.getAbsolutePath}'\n\toutput path: '${settings.outputTransactionsFile.getAbsolutePath}'\n\tkeystore path: '${settings.keystoreFile.getAbsolutePath}'\n\tchain char: '${settings.chainByte.toChar}'")
    val wallet = initCrypto(settings)

    val signedTransactions = TransactionsSigner.readAndSign(settings.inputTransactionsFile, wallet)
    val signedJsons =
      signedTransactions.map(broadcastableData => broadcastableData.tx.json() + ("certificates" -> Json.toJson(broadcastableData.certificates)))
    val jsArray = JsArray(signedJsons)

    writeJsonToFile(jsArray, settings.outputTransactionsFile)
  }

  private def initCrypto(settings: TxSignerSettings): Wallet.WalletWithKeystore = {
    val cryptoSettings = cryptoSettingsFromString(settings.cryptoType) match {
      case Right(cryptoSettings: CryptoSettings) => cryptoSettings
      case Left(message) =>
        logger.error(message)
        sys.exit(1)
    }

    CryptoInitializer.init(cryptoSettings).valueOr(error => throw new RuntimeException(error.message))
    AddressScheme.setAddressSchemaByte(settings.chainByte.toChar)

    try {
      val walletSettings = WalletSettings(Some(settings.keystoreFile), settings.keystorePassword)
      Wallet.apply(walletSettings)
    } catch {
      case ex: Exception =>
        logger.error(
          s"Failed to create Wallet from provided file '${settings.keystoreFile.getAbsolutePath}' and password, exception message: '${ex.getMessage}'")
        sys.exit(1)
    }
  }

  private def setLoggingLevel(level: Level): Unit = {
    val lc = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    lc.getLogger("ROOT").setLevel(level)
  }

  private def writeJsonToFile(jsValue: JsValue, file: File): Unit = {
    val writer = new BufferedWriter(new FileWriter(file))
    try {
      writer.write(jsValue.toString())
      writer.flush()
    } catch {
      case ex: Exception => logger.error(s"Failed to write result json to output file '${file.getAbsolutePath}', reason: '${ex.getMessage}'")
    } finally {
      Closeables.close(writer, false)
    }
  }

}
