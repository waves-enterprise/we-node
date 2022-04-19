package com.wavesenterprise.generator

import com.typesafe.config.ConfigFactory
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.CryptoInitializer
import com.wavesenterprise.settings.CryptoSettings
import com.wavesenterprise.settings.WEConfigReaders._
import com.wavesenterprise.utils.Base58
import monix.eval.Task
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.NameMapper

import java.io.{File, PrintWriter}

case class ApiKeyHashGeneratorSettings(wavesCrypto: Boolean, apiKey: String, file: Option[String]) {
  val cryptoSettings: CryptoSettings = CryptoSettings.WavesCryptoSettings
}

object ApiKeyHashGenerator extends BaseGenerator[Unit] {

  implicit val readConfigInHyphen: NameMapper = net.ceedubs.ficus.readers.namemappers.implicits.hyphenCase // IDEA bug

  override def exceptionHandlers: PartialFunction[Throwable, Unit] = PartialFunction.empty

  override def generateFlow(args: Array[String]): Task[Unit] = Task {
    val configPath   = args.headOption.fold(exitWithError("Configuration file path not specified!"))(identity)
    val parsedConfig = ConfigFactory.parseFile(new File(configPath))
    val config       = ConfigFactory.load(parsedConfig).as[ApiKeyHashGeneratorSettings]("apikeyhash-generator")

    CryptoInitializer.init(config.cryptoSettings).left.foreach(error => exitWithError(error.message))

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
