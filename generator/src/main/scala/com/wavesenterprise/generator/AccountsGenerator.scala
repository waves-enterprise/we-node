package com.wavesenterprise.generator

import cats.Show
import cats.implicits.showInterpolator
import com.google.common.io.{CharStreams, Closeables}
import com.wavesenterprise.account.AddressScheme
import com.wavesenterprise.crypto.CryptoInitializer
import com.wavesenterprise.settings.{CryptoSettings, WalletSettings, Waves}
import com.wavesenterprise.utils.Console.readPasswordFromConsoleWithVerify
import com.wavesenterprise.wallet.Wallet
import monix.eval.Task
import net.ceedubs.ficus.readers.{EnumerationReader, NameMapper}
import pureconfig.generic.auto._
import pureconfig.ConfigSource

import java.io.{BufferedReader, File, InputStreamReader}
import java.net.{HttpURLConnection, MalformedURLException, URL}

case class AccountsGeneratorSettings(
    chainId: String,
    amount: Int,
    wallet: String,
    walletPassword: String,
    reloadNodeWallet: ReloadNodeWallet
) {
  val addressScheme: Char = chainId.head
}

object AccountsGeneratorSettings {

  val configPath: String                 = "accounts-generator"
  val reloadNodeWalletConfigPath: String = configPath + ".reload-node-wallet"

  implicit def toPrintable(implicit crypto: CryptoSettings): Show[AccountsGeneratorSettings] = { x =>
    import x._

    s"""
       |crypto:           $crypto
       |chainId:          $chainId
       |amount:           $amount
       |wallet:           $wallet
       |walletPassword:   $walletPassword
       |reloadNodeWallet: ${show"$reloadNodeWallet"}
       """.stripMargin
  }
}

case class ReloadNodeWallet(enabled: Boolean, url: String)
object ReloadNodeWallet {
  implicit val toPrintable: Show[ReloadNodeWallet] = { x =>
    import x._
    if (enabled) s"url = $url" else "disabled"
  }
}

object AccountsGeneratorApp extends EnumerationReader with BaseGenerator[Unit] {

  implicit val readConfigInHyphen: NameMapper = net.ceedubs.ficus.readers.namemappers.implicits.hyphenCase // IDEA bug

  override def exceptionHandlers: PartialFunction[Throwable, Unit] = {
    case _: MalformedURLException =>
      log.error(s"Looks like URL is wrong in config parameter '${AccountsGeneratorSettings.reloadNodeWalletConfigPath}.url'")
  }

  def generateFlow(args: Array[String]): Task[Unit] = Task {
    val configPath = args.headOption.fold(exitWithError("Configuration file path not specified!"))(identity)

    val configFile = new File(configPath)
    if (!configFile.exists()) {
      exitWithError(s"Configuration file '$configPath' does not exist!")
    }

    implicit val cryptoSettings: CryptoSettings = ConfigSource
      .file(configFile)
      .at(AccountsGeneratorSettings.configPath)
      .loadOrThrow[CryptoSettings]

    val config = ConfigSource
      .file(configFile)
      .at(AccountsGeneratorSettings.configPath)
      .loadOrThrow[AccountsGeneratorSettings]

    log.info(s"The final configuration: ${show"$config"}")

    CryptoInitializer.init(cryptoSettings).left.foreach(error => exitWithError(error.message))
    AddressScheme.setAddressSchemaByte(config.addressScheme)

    // create wallet file if not exists
    val walletFile = new File(config.wallet)
    if (!walletFile.exists() && cryptoSettings.isInstanceOf[Waves]) {
      walletFile.createNewFile()
    }

    val w = Wallet(WalletSettings(Some(walletFile), config.walletPassword))

    (1 to config.amount).foreach { i =>
      val maybePassword =
        readPasswordFromConsoleWithVerify(s"Please enter password for $i account (empty password means no password): ").toOption.filter(_.nonEmpty)
      val maybeAcc = w.generateNewAccount(maybePassword)
      maybeAcc.foreach { acc =>
        log.info(s"$i Address: ${acc.address}; public key: ${acc.publicKeyBase58}")
      }
    }
    reloadNodeWallet(config.reloadNodeWallet)
  }

  private def reloadNodeWallet(reloadNodeWallet: ReloadNodeWallet): Unit = {
    try {
      if (reloadNodeWallet.enabled) {
        val url = new URL(reloadNodeWallet.url)
        val con = url.openConnection.asInstanceOf[HttpURLConnection]
        con.setRequestMethod("POST")
        con.setRequestProperty("Accept-Language", "UTF-8")
        con.setDoOutput(false)

        val responseCode = con.getResponseCode
        log.info("\nSending reload wallet request to URL : " + url)
        log.info("Response Code : " + responseCode)

        var in: BufferedReader = null
        try {
          in = new BufferedReader(new InputStreamReader(con.getInputStream))
          val response = CharStreams.toString(in)
          log.info(response)
        } finally {
          if (in != null) {
            Closeables.closeQuietly(in)
          }
        }
      }
    } catch {
      case ex: java.net.ConnectException => log.error(s"Failed to connect to '${reloadNodeWallet.url}', because: ${ex.getMessage}")
    }
  }
  override def internalClose(): Unit = {}
}
