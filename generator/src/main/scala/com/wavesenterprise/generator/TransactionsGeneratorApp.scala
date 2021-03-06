package com.wavesenterprise.generator

import cats.implicits.showInterpolator
import com.typesafe.config.ConfigFactory
import com.wavesenterprise.account.{AddressScheme, PrivateKeyAccount}
import com.wavesenterprise.crypto.CryptoInitializer
import com.wavesenterprise.generator.GeneratorSettings.NodeAddress
import com.wavesenterprise.generator.cli.ScoptImplicits
import com.wavesenterprise.generator.config.{FicusImplicits, WorkerMode}
import com.wavesenterprise.network.client.NetworkSender
import com.wavesenterprise.network.handshake.SignedHandshakeV2
import com.wavesenterprise.settings.WEConfigReaders._
import com.wavesenterprise.settings.{CryptoSettings, FeeSettings, VersionConstants}
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.utils.NTP
import com.wavesenterprise.{ApplicationInfo, NodeVersion, OwnerCredentials, Version, crypto}
import monix.eval.{Coeval, Task}
import monix.execution.Scheduler
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.{EnumerationReader, NameMapper, ValueReader}
import org.asynchttpclient.AsyncHttpClient
import org.asynchttpclient.Dsl.asyncHttpClient
import pureconfig.ConfigSource
import scopt.OptionParser

import java.io.File
import java.net.InetSocketAddress
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Random

object TransactionsGeneratorApp extends BaseGenerator[Seq[Unit]] with ScoptImplicits with FicusImplicits with EnumerationReader {
  // IDEA BUG
  private implicit val inetSocketAddressReader: ValueReader[InetSocketAddress] = com.wavesenterprise.settings.inetSocketAddressReader
  private implicit val readConfigInHyphen: NameMapper                          = net.ceedubs.ficus.readers.namemappers.implicits.hyphenCase
  private implicit val httpClient: AsyncHttpClient                             = asyncHttpClient()

  override def exceptionHandlers: PartialFunction[Throwable, Unit] = {
    case ex: java.io.IOException if ex.getMessage.contains("Errors during sending transactions") =>
      log.error("Probably your address is not in a privacy network")
  }

  var sender: NetworkSender              = _
  var time: NTP                          = _
  var workersThreadPool: ExecutorService = _

  override def generateFlow(args: Array[String]): Task[Seq[Unit]] = {
    val configPath   = args.headOption.fold(exitWithError("Configuration file path not specified!"))(identity)
    val parsedConfig = ConfigFactory.parseFile(new File(configPath))
    val schemaChar   = parsedConfig.getString("generator.chain-id").head
    AddressScheme.setAddressSchemaByte(schemaChar)
    val cryptoSettings: CryptoSettings = CryptoSettings.WavesCryptoSettings

    CryptoInitializer.init(cryptoSettings).left.foreach(error => exitWithError(error.message))

    val fees   = ConfigSource.fromConfig(parsedConfig).at("generator.fees").loadOrThrow[FeeSettings.FeesEnabled]
    val config = ConfigFactory.load(parsedConfig).as[GeneratorSettings]("generator")

    val result = parser.parse(args.tail, config) match {
      case None => Future.failed(new Exception("Failed to parse command line parameters"))
      case Some(finalConfig) =>
        log.info(show"The final configuration: \n$finalConfig")

        val generator: TransactionGenerator = finalConfig.mode match {
          case Mode.NARROW      => new NarrowTransactionGenerator(finalConfig.narrow, finalConfig.privateKeyAccounts, fees)
          case Mode.WIDE        => new WideTransactionGenerator(finalConfig.wide, finalConfig.privateKeyAccounts, fees)
          case Mode.DYN_WIDE    => new DynamicWideTransactionGenerator(finalConfig.dynWide, finalConfig.privateKeyAccounts, fees)
          case Mode.MULTISIG    => new MultisigTransactionGenerator(finalConfig.multisig, finalConfig.privateKeyAccounts, fees)
          case Mode.ORACLE      => new OracleTransactionGenerator(finalConfig.oracle, finalConfig.privateKeyAccounts, fees)
          case Mode.SWARM       => new SmartGenerator(finalConfig.swarm, finalConfig.privateKeyAccounts, fees)
          case Mode.DOCKER_CALL => new ContractCallGenerator(finalConfig.dockerCall, finalConfig.privateKeyAccounts, fees, httpClient)
        }

        workersThreadPool = Executors.newFixedThreadPool(Math.max(1, finalConfig.sendTo.size))
        implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(workersThreadPool)

        val chainId  = finalConfig.addressScheme
        val nodeName = "generator"

        val applicationInstanceInfo = ApplicationInfo(VersionConstants.ApplicationName + chainId,
                                                      NodeVersion(Version.VersionTuple),
                                                      config.consensusType,
                                                      nodeName,
                                                      nodeNonce = Random.nextLong(),
                                                      None)
        val handshakeMaker: Coeval[SignedHandshakeV2] = {
          val OwnerCredentials(ownerAddress, ownerPassword) = config.ownerCredentials
          val privateKeyOpt                                 = finalConfig.accounts.getKeyWithPassword(ownerAddress, Some(ownerPassword))

          if (privateKeyOpt.isEmpty) {
            log.error(s"Failed to get ownerKey from wallet by address ${ownerAddress.stringRepr}")
            close(1)
          }
          val ownerKey = privateKeyOpt.get
          Coeval.eval {
            val sessionKey = PrivateKeyAccount(crypto.generateSessionKeyPair())
            SignedHandshakeV2.createAndSign(applicationInstanceInfo, sessionKey, ownerKey)
          }
        }
        sender = new NetworkSender(config.trafficLogger, handshakeMaker)
        sys.addShutdownHook(sender.close())

        @volatile
        var currentMode: WorkerMode = WorkerMode.Continue

        sys.addShutdownHook {
          log.error("Stopping generator")
          currentMode = WorkerMode.Stop
        }

        if (finalConfig.worker.workingTime > Duration.Zero) {
          log.info(s"Generator will be stopped after ${finalConfig.worker.workingTime}")

          Scheduler.global.scheduleOnce(finalConfig.worker.workingTime) {
            log.warn(s"Stopping generator after: ${finalConfig.worker.workingTime}")
            currentMode = if (finalConfig.worker.stopWhenLoadEnds) WorkerMode.Stop else WorkerMode.Freeze
          }
        }

        val workers = finalConfig.sendTo.map {
          case NodeAddress(node, nodeRestUrl) =>
            log.info(s"Creating worker: ${node.getHostString}:${node.getPort}")
            new Worker(
              finalConfig.worker,
              Iterator.continually(generator.next()).flatten,
              sender,
              node,
              nodeRestUrl,
              () => currentMode,
              generator.initTxs.iterator,
              txs => generator.afterInitTxsValidation(txs, nodeRestUrl)
            )
        }

        time = new NTP(Seq("pool.ntp.org"))(monix.execution.Scheduler.global)
        val compilationHack = Future.sequence(workers.map(_.run()))
        compilationHack
    }

    Task.fromFuture(result)
  }

  def internalClose(): Unit = {
    if (sender != null) sender.close()
    if (time != null) time.close()
    if (workersThreadPool != null) workersThreadPool.shutdown()
  }

  val parser = new OptionParser[GeneratorSettings]("generator") {
    head("TransactionsGenerator - WE load testing transactions generator")
    opt[Int]('i', "iterations").valueName("<iterations>").text("number of iterations").action { (v, c) =>
      c.copy(worker = c.worker.copy(iterations = v))
    }
    opt[FiniteDuration]('d', "delay").valueName("<delay>").text("delay between iterations").action { (v, c) =>
      c.copy(worker = c.worker.copy(delay = v))
    }
    opt[Boolean]('r', "auto-reconnect").valueName("<true|false>").text("reconnect on errors").action { (v, c) =>
      c.copy(worker = c.worker.copy(autoReconnect = v))
    }
    help("help").text("display this help message")

    cmd("narrow")
      .action { (_, c) =>
        c.copy(mode = Mode.NARROW)
      }
      .text("Run transactions between pre-defined accounts")
      .children(
        opt[Int]("transactions").abbr("t").optional().text("number of transactions").action { (x, c) =>
          c.copy(narrow = c.narrow.copy(transactions = x))
        }
      )

    cmd("wide")
      .action { (_, c) =>
        c.copy(mode = Mode.WIDE)
      }
      .text("Run transactions those transfer funds to another accounts")
      .children(
        opt[Int]("transactions").abbr("t").optional().text("number of transactions").action { (x, c) =>
          c.copy(wide = c.wide.copy(transactions = x))
        },
        opt[Option[Int]]("limit-accounts").abbr("la").optional().text("limit recipients").action { (x, c) =>
          c.copy(wide = c.wide.copy(limitDestAccounts = x))
        }
      )

    cmd("dyn-wide")
      .action { (_, c) =>
        c.copy(mode = Mode.DYN_WIDE)
      }
      .text("Like wide, but the number of transactions is changed during the iteration")
      .children(
        opt[Int]("start").abbr("s").optional().text("initial amount of transactions").action { (x, c) =>
          c.copy(dynWide = c.dynWide.copy(start = x))
        },
        opt[Double]("grow-adder").abbr("g").optional().action { (x, c) =>
          c.copy(dynWide = c.dynWide.copy(growAdder = x))
        },
        opt[Int]("max").abbr("m").optional().action { (x, c) =>
          c.copy(dynWide = c.dynWide.copy(maxTxsPerRequest = Some(x)))
        },
        opt[Option[Int]]("limit-accounts").abbr("la").optional().text("limit recipients").action { (x, c) =>
          c.copy(dynWide = c.dynWide.copy(limitDestAccounts = x))
        }
      )

    cmd("multisig")
      .action { (_, c) =>
        c.copy(mode = Mode.MULTISIG)
      }
      .text("Multisig cycle of funding, initializng and sending funds back")
      .children(
        opt[Int]("transactions").abbr("t").optional().text("number of transactions").action { (x, c) =>
          c.copy(multisig = c.multisig.copy(transactions = x))
        },
        opt[Boolean]("first-run").abbr("first").optional().text("generate set multisig script transaction").action { (x, c) =>
          c.copy(multisig = c.multisig.copy(firstRun = x))
        },
      )

    cmd("oracle")
      .action { (_, c) =>
        c.copy(mode = Mode.ORACLE)
      }
      .text("Oracle load test")
      .children(
        opt[Int]("transactions").abbr("t").optional().text("number of transactions").action { (x, c) =>
          c.copy(oracle = c.oracle.copy(transactions = x))
        },
        opt[Boolean]("enabled").abbr("e").optional().text("DataEnty value").action { (x, c) =>
          c.copy(multisig = c.multisig.copy(firstRun = x))
        },
      )

    cmd("swarm")
      .action { (_, c) =>
        c.copy(mode = Mode.SWARM)
      }
      .text("SetScript load test")
      .children(
        opt[Int]("scripts").abbr("st").optional().text("number of SetScripts transactions").action { (x, c) =>
          c.copy(swarm = c.swarm.copy(scripts = x))
        },
        opt[Int]("transfers").abbr("tt").optional().text("number of Transfer transactions").action { (x, c) =>
          c.copy(swarm = c.swarm.copy(transfers = x))
        },
        opt[Boolean]("complexity").abbr("ct").optional().text(" script complexity").action { (x, c) =>
          c.copy(swarm = c.swarm.copy(complexity = x))
        },
        opt[Int]("exchange").abbr("et").optional().text("number of exchange transactions").action { (x, c) =>
          c.copy(swarm = c.swarm.copy(exchange = x))
        }
      )

    cmd("contract-call")
      .action { (_, c) =>
        c.copy(mode = Mode.DOCKER_CALL)
      }
      .text("Run transactions for deploy and call docker smart-contracts")
      .children(
        opt[Int]("transactions").abbr("t").optional().text("number of transactions").action { (x, c) =>
          c.copy(dockerCall = c.dockerCall.copy(transactions = x))
        },
        opt[String]("image").abbr("i").optional().text("contract docker image url").action { (x, c) =>
          c.copy(dockerCall = c.dockerCall.copy(image = x))
        },
        opt[String]("image-hash").abbr("ih").optional().text("contract docker image id").action { (x, c) =>
          c.copy(dockerCall = c.dockerCall.copy(imageHash = x))
        },
        opt[Seq[String]]("contract-ids").abbr("ci").optional().text("predefined call contract ids").action { (x, c) =>
          c.copy(dockerCall = c.dockerCall.copy(contractIds = x))
        },
        opt[List[DataEntry[_]]]("create-params").abbr("cr-p").optional().text("create contract params").action { (x, c) =>
          c.copy(dockerCall = c.dockerCall.copy(createParams = x))
        },
        opt[List[DataEntry[_]]]("call-params").abbr("c-p").optional().text("call contract params").action { (x, c) =>
          c.copy(dockerCall = c.dockerCall.copy(callParams = x))
        },
      )
  }
}
