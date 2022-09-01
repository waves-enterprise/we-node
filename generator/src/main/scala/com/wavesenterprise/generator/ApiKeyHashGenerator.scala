package com.wavesenterprise.generator

import com.typesafe.config.ConfigFactory
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.CryptoInitializer
import com.wavesenterprise.settings.CryptoSettings.cryptoSettingsFromString
import com.wavesenterprise.utils.Base58
import monix.eval.Task
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import cats.implicits.catsSyntaxEither

import java.io.{File, PrintWriter}

case class ApiKeyHashGeneratorSettings(
    crypto: String,
    apiKey: String,
    file: Option[String]
)

object ApiKeyHashGenerator extends BaseGenerator[Unit] {

  private val rootConfigSection = "apikeyhash-generator"

  override def exceptionHandlers: PartialFunction[Throwable, Unit] = PartialFunction.empty

  override def generateFlow(args: Array[String]): Task[Unit] = Task {
    val configPath     = args.headOption.fold(exitWithError("Configuration file path not specified!"))(identity)
    val parsedConfig   = ConfigFactory.parseFile(new File(configPath))
    val config         = ConfigSource.fromConfig(parsedConfig).at(rootConfigSection).loadOrThrow[ApiKeyHashGeneratorSettings]
    val cryptoSettings = cryptoSettingsFromString(config.crypto).valueOr(error => throw new RuntimeException(error))
    CryptoInitializer.init(cryptoSettings).left.foreach(error => exitWithError(error.message))

    val apiKeyHash = crypto.secureHash(config.apiKey)

    val resultString =
      s"""Api key: ${config.apiKey}
         |Api key hash: ${Base58.encode(apiKeyHash)}
       """.stripMargin

    config.file.foreach { outFile =>
      val writer = new PrintWriter(outFile, "UTF-8")
      writer.print(resultString)
      writer.flush()
      writer.close()
    }

    println(resultString)
  }

  override def internalClose(): Unit = {}
}
