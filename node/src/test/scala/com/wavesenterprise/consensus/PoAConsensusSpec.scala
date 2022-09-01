package com.wavesenterprise.consensus

import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.acl._
import com.wavesenterprise.block.Block
import com.wavesenterprise.consensus.MinerBanlistEntry.PriorWarningsInfo
import com.wavesenterprise.history._
import com.wavesenterprise.settings.BlockchainSettings
import com.wavesenterprise.settings.ConsensusSettings.PoASettings
import com.wavesenterprise.state.NG
import com.wavesenterprise.transaction.BlockchainUpdater
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.Time
import com.wavesenterprise.wallet.Wallet
import com.wavesenterprise.{NTPTime, NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.SortedSet
import scala.concurrent.duration._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class PoAConsensusSpec extends AnyFreeSpec with Matchers with ScalaCheckPropertyChecks with TransactionGen with NoShrink with NTPTime {
  import PoAConsensusSpec._

  val blockchainSettings: BlockchainSettings = DefaultWESettings.blockchain

  val signerAcc: PrivateKeyAccount = Wallet.generateNewAccount()

  val genMinersMap: Gen[Map[PrivateKeyAccount, Permissions]] =
    Gen.nonEmptyMap(for {
      acc        <- accountGen
      minerPerms <- PermissionsGen.minerPermissionsGen
    } yield acc -> minerPerms)

  "Select miner without skipping" - {
    "when previous miner is in the beginning of the list" in {

      val addrToPermMap: Map[Address, PermissionOp] = (1 to 10).toList.map { _ =>
        Wallet.generateNewAccount().toAddress -> PermissionOp(OpType.Add, Role.Miner, tick(), None)
      }.toMap + (signerAcc.toAddress -> PermissionOp(OpType.Add, Role.Miner, tick() - 100000, None))

      val sortedMiners: SortedSet[Address] = {
        implicit val newestPermissionOpOrd: Ordering[PermissionOp] = Permissions.ascendingOrdering
        implicit val ordering: Ordering[Address]                   = Ordering.by(addrToPermMap)

        addrToPermMap.keySet.to[SortedSet]
      }

      sortedMiners.firstKey shouldBe signerAcc.toAddress

      val blockchain = mockMyBlockchain(signerAcc, addrToPermMap)
      val time       = mock[Time]

      val poa = new PoAConsensus(blockchain, blockchainSettings, time, poaSettings)

      val nextMiner = poa.determineMiner(tick(), 10)

      nextMiner shouldBe Right(sortedMiners.iteratorFrom(signerAcc.toAddress).drop(1).next())
    }

    "when previous miner is at the end of the list" in {
      forAll(Gen.chooseNum(0, 10)) { minerCount =>
        val timeReference = tick()

        val firstMiner = Wallet.generateNewAccount()

        val addressToPermMap: Map[Address, PermissionOp] = (1 to minerCount).toList.map { i =>
          Wallet.generateNewAccount().toAddress -> PermissionOp(OpType.Add, Role.Miner, timeReference - i, None)
        }.toMap +
          (signerAcc.toAddress  -> PermissionOp(OpType.Add, Role.Miner, timeReference, None)) +
          (firstMiner.toAddress -> PermissionOp(OpType.Add, Role.Miner, 1L, None))

        val blockchain = mockMyBlockchain(signerAcc, addressToPermMap)
        val time       = mock[Time]

        val poa = new PoAConsensus(blockchain, blockchainSettings, time, poaSettings)

        val nextMiner = poa.determineMiner(tick(), 10)

        nextMiner shouldBe Right(firstMiner.toAddress)

      }
    }
  }

  "Select miner (with skips)" - {
    "skip one step" in {
      forAll(Gen.chooseNum(0, 10)) { minerCount =>
        val timeReference = tick()

        val minerToSkip       = Wallet.generateNewAccount()
        val expectedNextMiner = Wallet.generateNewAccount()

        val addressToPermMap: Map[Address, PermissionOp] = (1 to minerCount).toList.map { i =>
          Wallet.generateNewAccount().toAddress -> PermissionOp(OpType.Add, Role.Miner, timeReference - i, None)
        }.toMap +
          (signerAcc.toAddress         -> PermissionOp(OpType.Add, Role.Miner, timeReference, None)) +
          (minerToSkip.toAddress       -> PermissionOp(OpType.Add, Role.Miner, 1L, None)) +
          (expectedNextMiner.toAddress -> PermissionOp(OpType.Add, Role.Miner, 2L, None))

        val blockchain = mockMyBlockchain(signerAcc, addressToPermMap)
        val time       = mock[Time]

        val poa = new PoAConsensus(blockchain, blockchainSettings, time, poaSettings)

        val nextMiner = poa.determineMiner(tick(), 10, skipRounds = 1)

        nextMiner shouldBe Right(expectedNextMiner.toAddress)
      }
    }

    "skip more than length" in {
      forAll(Gen.chooseNum(0, 10)) { minerCount =>
        val timeReference = tick()

        val minerToSkip       = Wallet.generateNewAccount()
        val expectedNextMiner = Wallet.generateNewAccount()

        val addressToPermMap: Map[Address, PermissionOp] = (1 to minerCount).toList.map { i =>
          Wallet.generateNewAccount().toAddress -> PermissionOp(OpType.Add, Role.Miner, timeReference - i, None)
        }.toMap +
          (signerAcc.toAddress         -> PermissionOp(OpType.Add, Role.Miner, timeReference, None)) +
          (minerToSkip.toAddress       -> PermissionOp(OpType.Add, Role.Miner, 1L, None)) +
          (expectedNextMiner.toAddress -> PermissionOp(OpType.Add, Role.Miner, 2L, None))

        val skip = 10 * addressToPermMap.keys.size + 1

        val blockchain = mockMyBlockchain(signerAcc, addressToPermMap)
        val time       = mock[Time]

        val poa = new PoAConsensus(blockchain, blockchainSettings, time, poaSettings)

        val nextMiner = poa.determineMiner(tick(), 10, skipRounds = skip)

        nextMiner shouldBe Right(expectedNextMiner.toAddress)
      }
    }
  }

  "Select miner when there are banned miners in queue" - {
    "if the next miner is banned" in {
      forAll(accountGen, accountGen, Gen.chooseNum(2, 1000)) { (bannedMiner, expectedMiner, banHeight) =>
        val addressToPermMap: Map[Address, PermissionOp] =
          Map(
            signerAcc.toAddress     -> PermissionOp(OpType.Add, Role.Miner, 1L, None),
            bannedMiner.toAddress   -> PermissionOp(OpType.Add, Role.Miner, 2L, None),
            expectedMiner.toAddress -> PermissionOp(OpType.Add, Role.Miner, 3L, None)
          )

        val banHistoryMap =
          Map(
            signerAcc.toAddress     -> banHistoryBuilder.empty,
            bannedMiner.toAddress   -> banHistoryBuilder.build(Seq(MinerBanlistEntry.Ban(1234L, banHeight, PriorWarningsInfo(Array(123L, 234L))))),
            expectedMiner.toAddress -> banHistoryBuilder.empty
          )

        val blockchain = mockMyBlockchain(signerAcc, addressToPermMap, banHistoryMap)
        val time       = mock[Time]

        val poa = new PoAConsensus(blockchain, blockchainSettings, time, poaSettings)

        val nextMiner = poa.determineMiner(tick(), banHeight)

        nextMiner shouldBe Right(expectedMiner.toAddress)
      }
    }

    "if two next miners are banned" in {
      forAll(accountGen, accountGen, accountGen, Gen.chooseNum(2, 1000)) { (bannedMiner, anotherBannedMiner, expectedMiner, banHeight) =>
        val addressToPermMap: Map[Address, PermissionOp] =
          Map(
            signerAcc.toAddress          -> PermissionOp(OpType.Add, Role.Miner, 1L, None),
            bannedMiner.toAddress        -> PermissionOp(OpType.Add, Role.Miner, 2L, None),
            anotherBannedMiner.toAddress -> PermissionOp(OpType.Add, Role.Miner, 3L, None),
            expectedMiner.toAddress      -> PermissionOp(OpType.Add, Role.Miner, 3L, None)
          )

        val banHistoryMap =
          Map(
            signerAcc.toAddress   -> banHistoryBuilder.empty,
            bannedMiner.toAddress -> banHistoryBuilder.build(Seq(MinerBanlistEntry.Ban(1234L, banHeight, PriorWarningsInfo(Array(123L, 234L))))),
            anotherBannedMiner.toAddress -> banHistoryBuilder.build(
              Seq(MinerBanlistEntry.Ban(1235L, banHeight, PriorWarningsInfo(Array(123L, 234L))))),
            expectedMiner.toAddress -> banHistoryBuilder.empty
          )

        val blockchain = mockMyBlockchain(signerAcc, addressToPermMap, banHistoryMap)
        val time       = mock[Time]

        val poa = new PoAConsensus(blockchain, blockchainSettings, time, poaSettings)

        val nextMiner = poa.determineMiner(tick(), banHeight)

        nextMiner shouldBe Right(expectedMiner.toAddress)
      }
    }

    "if previous miner is removed (due miner permission)" in {
      forAll(accountGen, accountGen, accountGen, timestampGen) { (firstMiner, removedMiner, expectedMiner, checkTimestamp) =>
        val addressToPermMap: Map[Address, PermissionOp] =
          Map(
            firstMiner.toAddress    -> PermissionOp(OpType.Add, Role.Miner, 1L, None),
            removedMiner.toAddress  -> PermissionOp(OpType.Add, Role.Miner, 2L, Some(checkTimestamp - 1L)),
            expectedMiner.toAddress -> PermissionOp(OpType.Add, Role.Miner, 3L, None)
          )

        val blockchain = mockMyBlockchain(signerAcc, addressToPermMap)
        val time       = mock[Time]

        val poa = new PoAConsensus(blockchain, blockchainSettings, time, poaSettings)

        val nextMiner = poa.determineMiner(checkTimestamp, 1)

        nextMiner shouldBe Right(expectedMiner.toAddress)
      }
    }

    "if previous miner is banned, choose the next one" in {
      forAll(accountGen, Gen.listOf(accountGen), accountGen, Gen.choose(10, 10000)) { (firstMiner, bannedMiners, expectedMiner, banHeight) =>
        val addressToPermMap: Map[Address, PermissionOp] =
          (firstMiner +: bannedMiners :+ expectedMiner).zipWithIndex.map {
            case (account, idx) =>
              account.toAddress -> PermissionOp(OpType.Add, Role.Miner, idx + 1L, None)
          }.toMap

        val banHistoryMap: Map[Address, MinerBanHistory] = bannedMiners.map { account =>
          account.toAddress -> banHistoryBuilder.build(Seq(MinerBanlistEntry.Ban(3L, banHeight, PriorWarningsInfo(Array(1L, 2L)))))
        }.toMap +
          (firstMiner.toAddress    -> banHistoryBuilder.empty) +
          (expectedMiner.toAddress -> banHistoryBuilder.empty)

        val blockchain = mockMyBlockchain(firstMiner, addressToPermMap, banHistoryMap)

        val time = mock[Time]

        val poa = new PoAConsensus(blockchain, blockchainSettings, time, poaSettings)

        val maxMinerTimestamp = addressToPermMap.values.map(_.timestamp).max

        val nextMiner = poa.determineMiner(maxMinerTimestamp + 1L, banHeight + 1)

        nextMiner shouldBe Right(expectedMiner.toAddress)
      }
    }

    "current and next miners is correct" in {
      forAll(accountGen, Gen.listOf(accountGen), accountGen, Gen.choose(10, 10000)) { (firstMiner, bannedMiners, expectedMiner, banHeight) =>
        val addressToPermMap: Map[Address, PermissionOp] =
          (firstMiner +: bannedMiners :+ expectedMiner).zipWithIndex.map {
            case (account, idx) =>
              account.toAddress -> PermissionOp(OpType.Add, Role.Miner, idx + 1L, None)
          }.toMap

        val banHistoryMap: Map[Address, MinerBanHistory] = bannedMiners.map { account =>
          account.toAddress -> banHistoryBuilder.build(Seq(MinerBanlistEntry.Ban(3L, banHeight, PriorWarningsInfo(Array(1L, 2L)))))
        }.toMap +
          (firstMiner.toAddress    -> banHistoryBuilder.empty) +
          (expectedMiner.toAddress -> banHistoryBuilder.empty)

        val blockchain = mockMyBlockchain(firstMiner, addressToPermMap, banHistoryMap)
        (blockchain.height _).expects().returning(banHeight + 1).noMoreThanOnce()

        val time              = mock[Time]
        val maxMinerTimestamp = addressToPermMap.values.map(_.timestamp).max
        (time.correctedTime _).expects().returning(maxMinerTimestamp + 1).anyNumberOfTimes()

        val poa                        = new PoAConsensus(blockchain, blockchainSettings, ntpTime, poaSettings)
        val actualCurrentAndNextMiners = poa.getCurrentAndNextMiners().getOrElse(Seq())

        actualCurrentAndNextMiners should contain theSameElementsInOrderAs Seq(expectedMiner, firstMiner).map(_.toAddress)

      }
    }
  }

  "Block score" - {
    "always not negative" in {
      forAll(Gen.choose(0, Long.MaxValue), timestampGen) { (skippedRounds, timestamp) =>
        PoALikeConsensus.calculateBlockScore(skippedRounds, timestamp) should be > BigInt(0)
      }
    }

    "block with lower skipped rounds value not produce less score" in {
      forAll(Gen.choose(0, Long.MaxValue - 1), timestampGen) { (skippedRounds, timestamp) =>
        PoALikeConsensus.calculateBlockScore(skippedRounds, timestamp) should be >=
          PoALikeConsensus.calculateBlockScore(skippedRounds + 1, timestamp)
      }
    }

    "block with greater timestamp not produce less score" in {
      forAll(Gen.choose(0, Long.MaxValue), timestampGen) { (skippedRounds, timestamp) =>
        PoALikeConsensus.calculateBlockScore(skippedRounds, timestamp + 1) should be >=
          PoALikeConsensus.calculateBlockScore(skippedRounds, timestamp)
      }
    }
  }
}

object PoAConsensusSpec extends MockFactory {
  trait TestBlockchainUpdater extends BlockchainUpdater with NG

  val banDurationBlocks: Int = 100
  val warningsForBan: Int    = 3
  val maxBansPercentage: Int = 50
  val poaSettings            = PoASettings(60.seconds, 15.seconds, banDurationBlocks, warningsForBan, maxBansPercentage)

  implicit val minerBanHistoryOrdering = MinerBanHistory.modernBanlistEntryOrdering
  val banHistoryBuilder                = new MinerBanHistoryBuilderV2(banDurationBlocks, warningsForBan)

  def tick(): Long = System.currentTimeMillis()

  def mockMyBlockchain(minerAccount: PrivateKeyAccount, addressToPermissions: Map[Address, PermissionOp]): BlockchainUpdater with NG =
    mockMyBlockchain(minerAccount, addressToPermissions, Map.empty)

  def mockMyBlockchain(minerAccount: PrivateKeyAccount,
                       addressToPermissions: Map[Address, PermissionOp],
                       banHistory: Map[Address, MinerBanHistory]): BlockchainUpdater with NG = {

    val blockchain: BlockchainUpdater with NG = mock[PoAConsensusSpec.TestBlockchainUpdater]

    val signedBlockMock =
      Block.buildAndSign(Block.NgBlockVersion, tick(), randomSig, PoALikeConsensusBlockData(0), Seq.empty, minerAccount, Set.empty).explicitGet()

    val miners = MinerQueue(addressToPermissions.mapValues(permOp => Permissions(Seq(permOp))))

    (blockchain.miners _).expects().returning(miners).noMoreThanTwice()
    (blockchain.blockHeaderAndSize(_: Int)).expects(*).returning(Some((signedBlockMock.blockHeader, 0))).anyNumberOfTimes()
    (blockchain.lastBlock _).expects().returning(Some(signedBlockMock))

    if (banHistory.nonEmpty) {
      banHistory.foreach {
        case (address, minerBanHistory) =>
          (blockchain.minerBanHistory(_: Address)).expects(address).returning(minerBanHistory).anyNumberOfTimes()
      }
    } else {
      (blockchain.minerBanHistory(_: Address)).expects(*).returning(banHistoryBuilder.empty).anyNumberOfTimes()
    }

    blockchain
  }
}
