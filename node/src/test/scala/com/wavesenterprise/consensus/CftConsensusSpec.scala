package com.wavesenterprise.consensus

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.account.Address
import com.wavesenterprise.acl._
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.block.{Block, SignerData}
import com.wavesenterprise.consensus.PoAConsensusSpec.TestBlockchainUpdater
import com.wavesenterprise.settings.ConsensusSettings.CftSettings
import com.wavesenterprise.settings.{PositiveInt, TestBlockchainSettings}
import com.wavesenterprise.state._
import com.wavesenterprise.state.diffs.ProduceError.produce
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.Time
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class CftConsensusSpec extends AnyFreeSpec with Matchers with ScalaCheckPropertyChecks with MockFactory with TransactionGen {

  private def minerQueueWithActiveTsGen(minMinerCount: Int = 10,
                                        maxMinerCount: Int = 30,
                                        additionalAddresses: Seq[Address] = Seq.empty): Gen[(MinerQueue, Long)] =
    for {
      count     <- Gen.choose(minMinerCount, maxMinerCount)
      startTs   <- timestampGen
      addresses <- Gen.listOfN(count, addressGen)
    } yield {
      val activeTs = startTs + count
      val permissionsMap = (addresses ++ additionalAddresses).zipWithIndex.map {
        case (address, i) =>
          address -> Permissions(Seq(PermissionOp(OpType.Add, Role.Miner, startTs + i, None)))
      }.toMap

      MinerQueue(permissionsMap) -> activeTs
    }

  private def blockIdGen: Gen[BlockId] = bytes64gen.map(ByteStr(_))

  private val DefaultCftSettings = CftSettings(
    roundDuration = 10.seconds,
    syncDuration = 3.seconds,
    banDurationBlocks = 14,
    warningsForBan = 3,
    maxBansPercentage = 33,
    maxValidators = PositiveInt(7),
    fullVoteSetTimeout = None,
    finalizationTimeout = 3.seconds
  )

  "Expected validators" - {
    "are same for different runs" in {
      forAll(minerQueueWithActiveTsGen(), Gen.nonEmptyListOf(blockIdGen), blockIdGen) {
        case ((minerQueue, activeTs), lastBlockIds, parentBlockId) =>
          val blockchain = mock[TestBlockchainUpdater]
          (blockchain.miners _).expects().returns(minerQueue).twice()
          (blockchain.lastBlockIds(_: BlockId, _: Int)).expects(parentBlockId, *).returns(Some(lastBlockIds)).twice()

          val currentMiners = minerQueue.currentMinersSet(activeTs)
          val settings      = DefaultCftSettings.copy(maxValidators = PositiveInt(currentMiners.size / 2))
          val currentMiner  = currentMiners.head
          val time          = mock[Time]

          val consensus = new CftConsensus(blockchain, TestBlockchainSettings.Default, time, settings)

          consensus.expectedValidators(parentBlockId, activeTs, currentMiner) shouldBe
            consensus.expectedValidators(parentBlockId, activeTs, currentMiner)
      }
    }

    "are different for different last block ids" in {
      forAll(minerQueueWithActiveTsGen(minMinerCount = 30, maxMinerCount = 100), Gen.nonEmptyListOf(blockIdGen), blockIdGen, blockIdGen) {
        case ((minerQueue, activeTs), lastBlockIds, parentBlockId, additionalBlockId) =>
          val blockchain = mock[TestBlockchainUpdater]
          (blockchain.miners _).expects().returns(minerQueue).twice()
          (blockchain.lastBlockIds(_: BlockId, _: Int)).expects(parentBlockId, *).returns(Some(lastBlockIds)).once()
          (blockchain.lastBlockIds(_: BlockId, _: Int)).expects(parentBlockId, *).returns(Some(additionalBlockId +: lastBlockIds)).once()

          val currentMiners = minerQueue.currentMinersSet(activeTs)
          val settings      = DefaultCftSettings.copy(maxValidators = PositiveInt(currentMiners.size / 4))
          val currentMiner  = currentMiners.head
          val time          = mock[Time]

          val consensus = new CftConsensus(blockchain, TestBlockchainSettings.Default, time, settings)

          consensus.expectedValidators(parentBlockId, activeTs, currentMiner).explicitGet() should not be
            consensus.expectedValidators(parentBlockId, activeTs, currentMiner).explicitGet()
      }
    }

    "are all active miners if their number does not exceed maxValidators" in {
      forAll(minerQueueWithActiveTsGen(), blockIdGen) {
        case ((minerQueue, activeTs), parentBlockId) =>
          val blockhchain = mock[TestBlockchainUpdater]
          (blockhchain.miners _).expects().returns(minerQueue).once()
          (blockhchain.lastBlockIds(_: BlockId, _: Int)).expects(*, *).never()

          val currentMiners = minerQueue.currentMinersSet(activeTs)
          val settings      = DefaultCftSettings.copy(maxValidators = PositiveInt(currentMiners.size))
          val currentMiner  = currentMiners.head
          val time          = mock[Time]

          val consensus = new CftConsensus(blockhchain, TestBlockchainSettings.Default, time, settings)

          consensus.expectedValidators(parentBlockId, activeTs, currentMiner).explicitGet() shouldEqual (currentMiners - currentMiner)
      }
    }
  }

  "Block validation" - {
    "fails if parent block has no votes" in {
      forAll(timestampGen, blockIdGen, blockIdGen, accountGen, Gen.nonEmptyListOf(randomTransactionGen)) {
        case (ts, parentId, parentReferenceId, blockSigner, txs) =>
          val validationBlock = Block
            .build(Block.NgBlockVersion,
                   ts,
                   parentId,
                   CftLikeConsensusBlockData(Seq.empty, 0),
                   txs,
                   SignerData(blockSigner, ByteStr.empty),
                   Set.empty)
            .explicitGet()

          val parentBlock = Block
            .buildAndSign(Block.NgBlockVersion, ts, parentReferenceId, CftLikeConsensusBlockData(Seq.empty, 0), Seq.empty, blockSigner, Set.empty)
            .explicitGet()

          val blockchain = mock[TestBlockchainUpdater]
          (blockchain.blockHeaderByIdWithLiquidVariations(_: ByteStr)).expects(parentId).returns(Some(parentBlock.blockHeader)).once()
          val time = mock[Time]

          val consensus = new CftConsensus(blockchain, TestBlockchainSettings.Default, time, DefaultCftSettings)

          consensus.blockConsensusValidation(ts, validationBlock) should produce(s"Parent block '$parentId' is not finalized")
      }
    }

    "fails if block has not enough votes" in {
      val testVoteCount                 = 1
      val unsatisfactoryValidatorsCount = math.ceil(testVoteCount * (1 / CftConsensus.CftRatio) + 1).toInt
      val preconditions =
        for {
          parentId          <- blockIdGen
          parentReferenceId <- blockIdGen
          parentVersion     <- Gen.oneOf(Block.GenesisBlockVersions)
          blockSigner       <- accountGen
          voteSigner        <- accountGen
          txs               <- Gen.nonEmptyListOf(randomTransactionGen)
          (minerQueue, ts) <-
            minerQueueWithActiveTsGen(minMinerCount = unsatisfactoryValidatorsCount + 10, additionalAddresses = Seq(voteSigner.toAddress))
          lastBlockIds <- Gen.nonEmptyListOf(blockIdGen)
        } yield (parentId, parentReferenceId, parentVersion, blockSigner, voteSigner, txs, minerQueue, ts, lastBlockIds)

      forAll(preconditions) {
        case (parentId, parentReferenceId, parentVersion, blockSigner, voteSigner, txs, minerQueue, ts, lastBlockIds) =>
          val blockWithoutVotes = Block
            .build(Block.NgBlockVersion,
                   ts,
                   parentId,
                   CftLikeConsensusBlockData(Seq.empty, 0),
                   txs,
                   SignerData(blockSigner, ByteStr.empty),
                   Set.empty)
            .explicitGet()

          val vote = Vote.buildAndSign(voteSigner, blockWithoutVotes.votingHash()).explicitGet()

          val validationBlock =
            Block.buildAndSign(Block.NgBlockVersion, ts, parentId, CftLikeConsensusBlockData(Seq(vote), 0), txs, blockSigner, Set.empty).explicitGet()

          val parentBlock = Block
            .buildAndSign(parentVersion, ts, parentReferenceId, CftLikeConsensusBlockData(Seq.empty, 0), Seq.empty, blockSigner, Set.empty)
            .explicitGet()

          val blockchain = mock[TestBlockchainUpdater]
          (blockchain.miners _).expects().returns(minerQueue).once()
          (blockchain.lastBlockIds(_: BlockId, _: Int)).expects(parentId, *).returns(Some(lastBlockIds)).once()
          (blockchain.blockHeaderByIdWithLiquidVariations(_: ByteStr)).expects(parentId).returns(Some(parentBlock.blockHeader)).once()

          val cftSettings = DefaultCftSettings.copy(maxValidators = PositiveInt(unsatisfactoryValidatorsCount))
          val time        = mock[Time]
          val consensus   = new CftConsensus(blockchain, TestBlockchainSettings.Default, time, cftSettings)

          consensus.blockConsensusValidation(ts, validationBlock) should produce("does not contain the required percent of votes")
      }
    }

    "fails if block has not enough votes for the required block" in {
      val testVoteCount               = 1
      val satisfactoryValidatorsCount = math.ceil(testVoteCount * (1 / CftConsensus.CftRatio) - 1).toInt
      val preconditions =
        for {
          parentId          <- blockIdGen
          parentReferenceId <- blockIdGen
          parentVersion     <- Gen.oneOf(Block.GenesisBlockVersions)
          blockSigner       <- accountGen
          voteSigner        <- accountGen
          voteHash          <- bytes32gen
          txs               <- Gen.nonEmptyListOf(randomTransactionGen)
          (minerQueue, ts)  <- minerQueueWithActiveTsGen(minMinerCount = satisfactoryValidatorsCount, additionalAddresses = Seq(voteSigner.toAddress))
          lastBlockIds      <- Gen.nonEmptyListOf(blockIdGen)
        } yield (parentId, parentReferenceId, parentVersion, blockSigner, voteSigner, voteHash, txs, minerQueue, ts, lastBlockIds)

      forAll(preconditions) {
        case (parentId, parentReferenceId, parentVersion, blockSigner, voteSigner, voteHash, txs, minerQueue, ts, lastBlockIds) =>
          val vote = Vote.buildAndSign(voteSigner, ByteStr(voteHash)).explicitGet()

          val validationBlock =
            Block.buildAndSign(Block.NgBlockVersion, ts, parentId, CftLikeConsensusBlockData(Seq(vote), 0), txs, blockSigner, Set.empty).explicitGet()

          val parentBlock = Block
            .buildAndSign(parentVersion, ts, parentReferenceId, CftLikeConsensusBlockData(Seq.empty, 0), Seq.empty, blockSigner, Set.empty)
            .explicitGet()

          val blockchain = mock[TestBlockchainUpdater]
          (blockchain.miners _).expects().returns(minerQueue).once()
          (blockchain.lastBlockIds(_: BlockId, _: Int)).expects(parentId, *).returns(Some(lastBlockIds)).once()
          (blockchain.blockHeaderByIdWithLiquidVariations(_: ByteStr)).expects(parentId).returns(Some(parentBlock.blockHeader)).once()

          val cftSettings = DefaultCftSettings.copy(maxValidators = PositiveInt(satisfactoryValidatorsCount))
          val time        = mock[Time]
          val consensus   = new CftConsensus(blockchain, TestBlockchainSettings.Default, time, cftSettings)

          consensus.blockConsensusValidation(ts, validationBlock) should produce("does not contain the required percent of votes")
      }
    }

    "fails if block has not enough votes with valid signature" in {
      val testVoteCount               = 1
      val satisfactoryValidatorsCount = math.ceil(testVoteCount * (1 / CftConsensus.CftRatio) - 1).toInt
      val preconditions =
        for {
          parentId          <- blockIdGen
          parentReferenceId <- blockIdGen
          parentVersion     <- Gen.oneOf(Block.GenesisBlockVersions)
          blockSigner       <- accountGen
          voteSigner        <- accountGen
          voteSignature     <- bytes64gen
          txs               <- Gen.nonEmptyListOf(randomTransactionGen)
          (minerQueue, ts)  <- minerQueueWithActiveTsGen(minMinerCount = satisfactoryValidatorsCount, additionalAddresses = Seq(voteSigner.toAddress))
          lastBlockIds      <- Gen.nonEmptyListOf(blockIdGen)
        } yield (parentId, parentReferenceId, parentVersion, blockSigner, voteSigner, voteSignature, txs, minerQueue, ts, lastBlockIds)

      forAll(preconditions) {
        case (parentId, parentReferenceId, parentVersion, blockSigner, voteSigner, voteSignature, txs, minerQueue, ts, lastBlockIds) =>
          val blockWithoutVotes = Block
            .build(Block.NgBlockVersion,
                   ts,
                   parentId,
                   CftLikeConsensusBlockData(Seq.empty, 0),
                   txs,
                   SignerData(blockSigner, ByteStr.empty),
                   Set.empty)
            .explicitGet()

          val vote = Vote(voteSigner, blockWithoutVotes.votingHash(), ByteStr(voteSignature))

          val validationBlock =
            Block.buildAndSign(Block.NgBlockVersion, ts, parentId, CftLikeConsensusBlockData(Seq(vote), 0), txs, blockSigner, Set.empty).explicitGet()

          val parentBlock = Block
            .buildAndSign(parentVersion, ts, parentReferenceId, CftLikeConsensusBlockData(Seq.empty, 0), Seq.empty, blockSigner, Set.empty)
            .explicitGet()

          val blockchain = mock[TestBlockchainUpdater]
          (blockchain.miners _).expects().returns(minerQueue).once()
          (blockchain.lastBlockIds(_: BlockId, _: Int)).expects(parentId, *).returns(Some(lastBlockIds)).once()
          (blockchain.blockHeaderByIdWithLiquidVariations(_: ByteStr)).expects(parentId).returns(Some(parentBlock.blockHeader)).once()

          val cftSettings = DefaultCftSettings.copy(maxValidators = PositiveInt(satisfactoryValidatorsCount))
          val time        = mock[Time]
          val consensus   = new CftConsensus(blockchain, TestBlockchainSettings.Default, time, cftSettings)

          consensus.blockConsensusValidation(ts, validationBlock) should produce("does not contain the required percent of votes")
      }
    }
  }
}
