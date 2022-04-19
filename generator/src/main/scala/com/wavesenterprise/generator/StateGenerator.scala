package com.wavesenterprise.generator

import com.wavesenterprise.Schedulers
import com.wavesenterprise.account.{AddressSchemeHelper, PrivateKeyAccount}
import com.wavesenterprise.block.Block
import com.wavesenterprise.consensus.{ConsensusPostAction, PoSLikeConsensusBlockData}
import com.wavesenterprise.crypto.CryptoInitializer
import com.wavesenterprise.database.rocksdb.{RocksDBStorage, RocksDBWriter}
import com.wavesenterprise.generator.transaction.{DataTransactionGenerator, DataTransactionGeneratorSettings, TransactionTypesSettings}
import com.wavesenterprise.settings.{CryptoSettings, FeeSettings, WESettings}
import com.wavesenterprise.state.{BlockchainUpdaterImpl, ByteStr}
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.assets.{BurnTransactionV2, IssueTransactionV2, ReissueTransactionV2}
import com.wavesenterprise.transaction.docker.{CallContractTransactionV2, CreateContractTransactionV1, ExecutedContractTransactionV1}
import com.wavesenterprise.transaction.lease.{LeaseCancelTransactionV2, LeaseTransactionV2}
import com.wavesenterprise.transaction.transfer.{MassTransferTransactionV1, MassTransferTransactionV2, TransferTransactionV2}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.Time
import com.wavesenterprise.wallet.Wallet
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.Scheduler.io
import monix.execution.schedulers.SchedulerService
import org.apache.commons.codec.digest.DigestUtils
import pureconfig.ConfigSource

object StateGeneratorSchedulers extends Schedulers {
  private val scheduler: SchedulerService = io("state-generator-scheduler")

  override val blockLoaderScheduler: Scheduler               = scheduler
  override val microBlockLoaderScheduler: Scheduler          = scheduler
  override val syncChannelSelectorScheduler: Scheduler       = scheduler
  override val appenderScheduler: Scheduler                  = scheduler
  override val historyRepliesScheduler: Scheduler            = scheduler
  override val minerScheduler: Scheduler                     = scheduler
  override val anchoringScheduler: Scheduler                 = scheduler
  override val dockerExecutorScheduler: Scheduler            = scheduler
  override val transactionBroadcastScheduler: Scheduler      = scheduler
  override val policyScheduler: Scheduler                    = scheduler
  override val cryptoServiceScheduler: Scheduler             = scheduler
  override val utxPoolSyncScheduler: Scheduler               = scheduler
  override val discardingHandlerScheduler: Scheduler         = scheduler
  override val channelProcessingScheduler: Scheduler         = scheduler
  override val messageObserverScheduler: Scheduler           = scheduler
  override val contractExecutionMessagesScheduler: Scheduler = scheduler
  override val validatorResultsHandlerScheduler: Scheduler   = scheduler
  override val ntpTimeScheduler: Scheduler                   = scheduler
  override val utxPoolBackgroundScheduler: Scheduler         = scheduler
  override val blockchainUpdatesScheduler: Scheduler         = scheduler
  override val signaturesValidationScheduler: Scheduler      = scheduler
  override val blockVotesHandlerScheduler: Scheduler         = scheduler
  override val healthCheckScheduler: Scheduler               = scheduler
  override val consensualSnapshotScheduler: Scheduler        = scheduler
  override val apiComputationsScheduler: Scheduler           = scheduler
}

/**
  * Generates blocks with transactions and writes them directly to RocksDB
  */
object StateGenerator extends BaseGenerator[Unit] {
  private val generatorConfig = NarrowTransactionGenerator.Settings(
    100,
    Map(
      IssueTransactionV2        -> 0.1,
      TransferTransactionV2     -> 0.2,
      ReissueTransactionV2      -> 0.05,
      BurnTransactionV2         -> 0.05,
      LeaseTransactionV2        -> 0.1,
      LeaseCancelTransactionV2  -> 0.05,
      CreateAliasTransactionV2  -> 0.1,
      CreateAliasTransactionV3  -> 0.1,
      MassTransferTransactionV1 -> 0.15,
      MassTransferTransactionV2 -> 0.15,
      DataTransactionV1         -> 0.3,
      DataTransactionV2         -> 0.3
    ),
    TransactionTypesSettings(
      DataTransactionGeneratorSettings(
        entryCount = 10,
        Map(
          "string"  -> 0.4,
          "binary"  -> 0.1,
          "integer" -> 0.3,
          "boolean" -> 0.2
        ),
        10, // in bytes
        30 // in bytes
      )),
    Seq.empty
  )

  val time: Time = new Time {
    def correctedTime(): Long = System.currentTimeMillis()

    def getTimestamp(): Long = System.currentTimeMillis()
  }

  var dbStorage: RocksDBStorage  = _
  var bcu: BlockchainUpdaterImpl = _

  override def generateFlow(args: Array[String]): Task[Unit] = {
    Task {
      log.info("Starting state generator")
      val (config, _) = com.wavesenterprise.readConfigOrTerminate(args.headOption)
      val fees        = ConfigSource.fromConfig(config).at("generator.fees").loadOrThrow[FeeSettings.FeesEnabled]
      val dataTxGen   = new DataTransactionGenerator(generatorConfig.transactionTypesSettings.dataTransaction, fees)

      val cryptoSettings = ConfigSource.fromConfig(config).at(WESettings.configPath).loadOrThrow[CryptoSettings]
      CryptoInitializer.init(cryptoSettings).left.foreach(error => exitWithError(error.message))
      AddressSchemeHelper.setAddressSchemaByte(config)
      val weConfig = ConfigSource.fromConfig(config).at(WESettings.configPath).loadOrThrow[WESettings]
      dbStorage = RocksDBStorage.openDB(weConfig.dataDirectory)

      val blockchain =
        new RocksDBWriter(dbStorage, weConfig.blockchain.custom.functionality, weConfig.blockchain.consensus, 100000, 2000)
      bcu = new BlockchainUpdaterImpl(blockchain, weConfig, time, StateGeneratorSchedulers)
      val wallet      = Wallet(weConfig.wallet)
      val txGenerator = new NarrowTransactionGenerator(generatorConfig, wallet.privateKeyAccounts, fees)

      def processBlock = (block: Block) => bcu.processBlock(block, ConsensusPostAction.NoAction)

      val genesisResult = for {
        genesisSettings <- weConfig.blockchain.custom.genesis.toPlainSettings
        genesis         <- Block.genesis(genesisSettings, weConfig.blockchain.consensus.consensusType)
        result          <- processBlock(genesis)
      } yield result

      genesisResult.left.foreach(err => exitWithError(err.toString))

      val randomTxs = txGenerator.generate(1500).sliding(3, 3).toList

      val result = wallet.privateKeyAccount(weConfig.ownerAddress, None) match {
        case Left(error) => exitWithError(error.toString)
        case Right(key) =>
          val dockerContracts = (1 to 5).map(_ => produceDockerCreate(key, dataTxGen))
          val txResults       = (randomTxs ::: List(dockerContracts)).map(txs => processBlock(produceBlock(txs, key)))

          val dockerCalls         = (1 to 5).map(_ => produceDockerCall(key, bcu.contracts().head.contractId, dataTxGen))
          val dockerCallsBlockRes = processBlock(produceBlock(dockerCalls, key))

          // empty block to satisfy ng state
          val emptyBlockRes = processBlock(produceBlock(Seq.empty, key))
          txResults ::: List(dockerCallsBlockRes, emptyBlockRes)
      }

      if (bcu.height > 1) log.info(s"State Successfully generated, current height is ${bcu.height}")
      else {
        log.error("Errors occurred during state generation")
        result.map(_.left).foreach(err => log.error(err.toString))
      }
    }
  }

  private def produceBlock(txs: Seq[Transaction], signer: PrivateKeyAccount): Block = {
    Block
      .buildAndSign(
        2,
        System.currentTimeMillis(),
        bcu.lastBlockId.get,
        PoSLikeConsensusBlockData(2L, ByteStr(Array.fill(Block.GeneratorSignatureLength)(0: Byte))),
        txs,
        signer,
        Set.empty[Short]
      )
      .explicitGet()
  }

  private def produceDockerCreate(signer: PrivateKeyAccount, dataTxGen: DataTransactionGenerator): ExecutedContractTransactionV1 = {
    val (params, results) = (0 until 10).map(dataTxGen.generateData).splitAt(5)

    ExecutedContractTransactionV1
      .selfSigned(
        signer,
        CreateContractTransactionV1
          .selfSigned(
            signer,
            "localhost:5000/smart-kv",
            DigestUtils.sha256Hex("some_data"),
            "some_name",
            params.toList,
            0,
            time.getTimestamp()
          )
          .explicitGet(),
        results.toList,
        time.getTimestamp()
      )
      .explicitGet()
  }

  private def produceDockerCall(signer: PrivateKeyAccount,
                                contractId: ByteStr,
                                dataTxGen: DataTransactionGenerator): ExecutedContractTransactionV1 = {
    val (params, results) = (0 until 10).map(dataTxGen.generateData).splitAt(5)

    ExecutedContractTransactionV1
      .selfSigned(signer,
                  CallContractTransactionV2
                    .selfSigned(
                      signer,
                      contractId,
                      params.toList,
                      0,
                      time.getTimestamp(),
                      1
                    )
                    .explicitGet(),
                  results.toList,
                  time.getTimestamp())
      .explicitGet()
  }

  override def exceptionHandlers: PartialFunction[Throwable, Unit] = PartialFunction.empty

  override def internalClose(): Unit = {
    bcu.shutdown()
    dbStorage.close()
  }
}
