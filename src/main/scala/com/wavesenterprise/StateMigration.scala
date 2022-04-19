package com.wavesenterprise

import com.google.common.io.ByteStreams.newDataOutput
import com.wavesenterprise.account.AddressSchemeHelper
import com.wavesenterprise.StateMigration.BlockNotFoundException
import com.wavesenterprise.block.{Block, BlockHeader, _}
import com.wavesenterprise.consensus.Consensus
import com.wavesenterprise.crypto.CryptoInitializer
import com.wavesenterprise.database.KeyHelpers.{h, hash}
import com.wavesenterprise.database.rocksdb.ColumnFamily.DefaultCF
import com.wavesenterprise.database.rocksdb._
import com.wavesenterprise.database.{Key, Keys, readTxIds}
import com.wavesenterprise.history.BlockchainFactory
import com.wavesenterprise.settings.{CryptoSettings, WESettings}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.state.appender.BaseAppender.BlockType.Hard
import com.wavesenterprise.utils.{NTP, ScorexLogging, Time}
import com.wavesenterprise.utils.NTPUtils.NTPExt
import monix.eval.{Coeval, Task}
import monix.execution.Scheduler.forkJoin
import monix.execution.schedulers.SchedulerService
import monix.execution.{CancelableFuture, Scheduler}
import monix.reactive.{Observable, OverflowStrategy}
import pureconfig.generic.semiauto.deriveReader
import pureconfig.{ConfigObjectSource, ConfigReader, ConfigSource}

import java.io.File
import java.nio.file.{Files, NoSuchFileException, Paths, StandardCopyOption}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, _}
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

case class MigrationSettings(sourcePath: String, targetPath: String, configPath: Option[String], bufferSize: Int, backpressure: Int)

object MigrationSettings {

  val configPath: String = "migration"

  implicit val configReader: ConfigReader[MigrationSettings] = deriveReader
}

object StateMigration extends App with ScorexLogging {

  case class BlockNotFoundException(height: Int) extends NoStackTrace

  val DefaultSettings: ConfigObjectSource = ConfigSource.string(s"""migration {
                    |  buffer-size = 10
                    |  backpressure = 30
                    |}""".stripMargin)

  val configSource = args.headOption.map(new File(_)) match {
    case Some(configFile) if configFile.exists() => ConfigSource.file(configFile)
    case Some(configFile)                        => exitWithError(s"Configuration file '$configFile' does not exist!")
    case None                                    => ConfigSource.empty
  }
  val migrationSettings = ConfigSource.defaultOverrides
    .withFallback(configSource)
    .withFallback(DefaultSettings)
    .at(MigrationSettings.configPath)
    .loadOrThrow[MigrationSettings]

  val (config, _)    = readConfigOrTerminate(migrationSettings.configPath)
  val cryptoSettings = ConfigSource.fromConfig(config).at(WESettings.configPath).loadOrThrow[CryptoSettings]
  CryptoInitializer.init(cryptoSettings).left.foreach(error => exitWithError(error.message))

  AddressSchemeHelper.setAddressSchemaByte(config)
  val weSettings = ConfigSource.fromConfig(config).at(WESettings.configPath).loadOrThrow[WESettings]

  val shutdownInitiated = new AtomicBoolean(false)

  var sourceDb: RocksDBStorage  = _
  var targetDb: RocksDBStorage  = _
  var schedulers: AppSchedulers = _
  var time: NTP                 = _
  var f: CancelableFuture[Unit] = _

  implicit val scheduler: SchedulerService =
    forkJoin(Runtime.getRuntime.availableProcessors(), 64, "migration-pool", reporter = log.error("Error in migration", _))

  sys.addShutdownHook { closeAll() }

  try {
    // Let's check if source directory exists and is non-empty
    Try(Files.list(Paths.get(migrationSettings.sourcePath))) match {
      // source directory is non-empty, expecting to find state there
      case Success(files) if files.findAny().isPresent =>
        Try {
          sourceDb = RocksDBStorage.openDB(migrationSettings.sourcePath, migrateScheme = false, MigrationSourceParams)
        } match {
          case Failure(ex) =>
            log.info("Failed to open source DB. Skipping migration", ex)
          case Success(_) if storageIsEmpty(sourceDb) =>
            log.info("Source DB is empty. Skipping migration")
          case Success(_) =>
            targetDb = RocksDBStorage.openDB(migrationSettings.targetPath, params = MigrationDestinationParams)
            schedulers = new AppSchedulers
            time = NTP(weSettings.blockchain.consensus.consensusType, weSettings.ntp)(schedulers.ntpTimeScheduler)

            // Open source DB with target parameters to handle a specific case when migration has already been done.
            val lazySourceAsTargetDb = Coeval.evalOnce {
              sourceDb.close()
              sourceDb = RocksDBStorage.openDB(migrationSettings.sourcePath, migrateScheme = false, MigrationDestinationParams)
              sourceDb
            }

            val migrationService = new MigrationService(sourceDb, targetDb, lazySourceAsTargetDb, weSettings, migrationSettings, schedulers, time)
            f = migrationService.runMigration(migrationSettings.bufferSize, migrationSettings.backpressure)

            Await.result(f, Duration.Inf)
            log.info("Migration finished successfully")
        }
      case Success(_) =>
        log.info("Source directory is empty. Skipping migration")
      case Failure(_: NoSuchFileException) =>
        log.info("Source directory doesn't exist. Skipping migration")
      case Failure(ex) =>
        throw ex
    }
  } catch {
    case ex: Throwable => exitWithError("Migration failed with error", Some(ex))
  } finally {
    closeAll()
  }

  def closeAll(): Unit = {
    if (shutdownInitiated.compareAndSet(false, true)) {
      Option(f).filterNot(_.isCompleted).foreach(_.cancel())
      AppSchedulers.shutdownAndWait(scheduler, "migration-pool", 2.minute)
      Option(time).foreach(_.close())
      Option(schedulers).foreach(_.shutdown())
      Option(sourceDb).foreach(_.close())
      Option(targetDb).foreach(_.close())
    }
  }

  def exitWithError(errorMessage: String, ex: Option[Throwable] = None): Nothing = {
    log.error(errorMessage, ex.orNull)
    sys.exit(1)
  }

  private def storageIsEmpty(storage: RocksDBStorage): Boolean = {
    val iterator = storage.newIterator()
    iterator.seekToFirst()
    val hasNext = iterator.isValid
    iterator.close()
    !hasNext
  }
}

class SourceBlockchain(override val storage: RocksDBStorage) extends ReadWriteDB {

  object LegacyKeys {
    def blockHeaderBytesAt(height: Int): Key[Option[Array[Byte]]] =
      Key.opt("block-header-bytes-at-height",
              DefaultCF,
              h(3, height),
              _.drop(4),
              _ => throw new Exception("Key \"block-header-bytes-at-height\" - is read only!"))

    def blockTransactionsAtHeight(height: Int): Key[Seq[ByteStr]] =
      Key("block-transaction-ids-at-height",
          DefaultCF,
          h(49, height),
          readTxIds,
          _ => throw new Exception("Key \"block-transaction-ids-at-height\" - is read only!"))

    def transactionBytes(txId: ByteStr): Key[Option[Array[Byte]]] =
      Key.opt("transaction-info-bytes",
              DefaultCF,
              hash(18, txId),
              _.drop(4),
              _ => throw new Exception("Key \"transaction-info-bytes\" - is read only!"))
  }

  def height: Int = readOnly(_.get(Keys.height))

  def blockAt(height: Int): Option[Block] = loadBlockBytes(height).map(parseBlockBytes)

  private def parseBlockBytes(bb: Array[Byte]): Block =
    Block.parseBytes(bb).fold(e => throw new RuntimeException("Can't parse block bytes", e), identity)

  private def loadBlockBytes(h: Int): Option[Array[Byte]] = readOnly { db =>
    val headerKey = LegacyKeys.blockHeaderBytesAt(h)
    db.get(headerKey).map { headerBytes =>
      val blockHeader = BlockHeader.parse(headerBytes)
      val txBytes     = readBlockTransactionBytes(h, db)
      val out         = newDataOutput(headerBytes.length + txBytes.length)
      out.writeAsBlockBytes(blockHeader, txBytes)
      out.toByteArray
    }
  }

  private def readBlockTransactionBytes(h: Int, db: ReadOnlyDB): Array[Byte] = {
    val out      = newDataOutput()
    val txIdList = db.get(LegacyKeys.blockTransactionsAtHeight(h))
    for (txId <- txIdList) {
      db.get(LegacyKeys.transactionBytes(txId))
        .map { txBytes =>
          out.writeInt(txBytes.length)
          out.write(txBytes)
        }
        .getOrElse(throw new RuntimeException(s"Cannot parse transaction with id '$txId' in block at height: $h"))
    }
    out.toByteArray
  }
}

class MigrationService(sourceDb: RocksDBStorage,
                       targetDb: RocksDBStorage,
                       sourceAsTargetDb: Coeval[RocksDBStorage],
                       weSettings: WESettings,
                       migrationSettings: MigrationSettings,
                       schedulers: Schedulers,
                       time: Time)(implicit val scheduler: Scheduler)
    extends ScorexLogging {

  private val sourceState      = new SourceBlockchain(sourceDb)
  private val (_, targetState) = BlockchainFactory(weSettings, targetDb, time, schedulers)
  private val consensus        = Consensus(weSettings.blockchain, targetState)

  def runMigration(bufferSize: Int, backpressure: Int): CancelableFuture[Unit] = {
    com.wavesenterprise.checkGenesis(weSettings, targetState)

    val sourceHeight = sourceState.height

    if (sourceAlreadyMigrated(sourceHeight)) {
      Task {
        log.warn(s"Migration has already been done, move source to target")

        sourceDb.close()
        targetDb.close()

        val targetDir = new File(migrationSettings.targetPath)

        Files
          .walk(Paths.get(migrationSettings.sourcePath))
          .iterator()
          .asScala
          .map(_.toFile)
          .filter(_.isFile)
          .foreach { sourceFile =>
            val destinationFile = new File(targetDir, sourceFile.getName)
            Files.move(sourceFile.toPath, destinationFile.toPath, StandardCopyOption.REPLACE_EXISTING)
          }
      }.runToFuture
    } else {
      val targetHeight = targetState.height

      val startHeight = targetHeight + 1
      log.info(s"Starting migration from height '$startHeight' to '$sourceHeight'...")

      Observable
        .range(startHeight, sourceHeight + 1)
        .mapParallelOrdered(parallelism = Runtime.getRuntime.availableProcessors()) { i =>
          Task {
            loadBlock(i.toInt)
          }
        }
        .executeAsync
        .executeOn(scheduler)
        .bufferTumbling(bufferSize)
        .asyncBoundary(OverflowStrategy.BackPressure(backpressure))
        .foreachL { batch =>
          batch.foreach(processBlock)
        }
        .runAsyncLogErr
    }
  }

  private def loadBlock(height: Int): (Int, Block) = {
    val block = blocking(sourceState.blockAt(height).getOrElse(throw BlockNotFoundException(height)))
    block.blockScore()
    block.bytes()
    //block.signatureValid()
    block.feesPortfolio()
    block.prevBlockFeePart()
    block.transactionData.foreach { tx =>
      tx.id()
      tx.bytes()
    }
    height -> block
  }

  private def processBlock(tuple: (Int, Block)): Unit = {
    val Tuple2(height, block) = tuple

    if (height % 1000 == 0) {
      log.info(s"Processing height '$height'")
    }
    blocking {
      consensus
        .calculatePostAction(block)
        .flatMap { postAction =>
          targetState.processBlock(block, postAction, Hard, isOwn = true)
        }
        .fold(ve => throw new RuntimeException(ve.toString), _ => ())
    }
  }

  /**
    * Checks if the migration invoke by mistake
    */
  private def sourceAlreadyMigrated(sourceHeight: Int): Boolean = {
    lazy val (_, sourceStateAsTarget) = BlockchainFactory(weSettings, sourceAsTargetDb(), time, schedulers)

    try {
      loadBlock(sourceHeight)
      false
    } catch {
      case BlockNotFoundException(height) if sourceStateAsTarget.blockAt(height).isDefined => true
    }
  }
}
