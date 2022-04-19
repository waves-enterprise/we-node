package com.wavesenterprise.state.diffs

import cats.implicits._
import com.wavesenterprise.acl.{OpType, Role}
import com.wavesenterprise.block.Block
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.db.WithDomain
import com.wavesenterprise.history._
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.state.{Blockchain, ParticipantRegistration}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.{GenesisTransaction, RegisterNodeTransactionV1}
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class RegisterNodeTransactionV1DiffTest
    extends PropSpec
    with ScalaCheckPropertyChecks
    with Matchers
    with TransactionGen
    with NoShrink
    with MockFactory
    with WithDomain {
  import RegisterNodeTransactionDiff._

  property("Adding participant that is already present returns Diff without registrations") {
    forAll(registerNodeTransactionGen(Gen.const(OpType.Add)), Gen.chooseNum(2, Int.MaxValue)) { (regNodeTx, height) =>
      val blockchain: Blockchain = mock[Blockchain]
      (blockchain.participantPubKey _).expects(regNodeTx.target.toAddress).returning(Some(regNodeTx.target)).anyNumberOfTimes()

      val maybeDiff = RegisterNodeTransactionDiff(blockchain, height)(regNodeTx)
      maybeDiff shouldBe Left(GenericError(s"Account '${regNodeTx.target}' already presented in blockchain"))
    }
  }

  property("Adding a new non-present participant returns Diff with registrations") {
    forAll(registerNodeTransactionGen(Gen.const(OpType.Add)), Gen.chooseNum(2, Int.MaxValue)) { (regNodeTx, height) =>
      val blockchain: Blockchain = mock[Blockchain]
      (blockchain.participantPubKey _).expects(regNodeTx.target.toAddress).returning(None).anyNumberOfTimes()

      val maybeDiff = RegisterNodeTransactionDiff(blockchain, height)(regNodeTx)
      maybeDiff.map(_.registrations) shouldBe Right(Seq(ParticipantRegistration(regNodeTx.target.toAddress, regNodeTx.target, OpType.Add)))
    }
  }

  property("Doesn't allow to remove node if it is not presented") {
    forAll(registerNodeTransactionGen(Gen.const(OpType.Remove)), Gen.chooseNum(2, Int.MaxValue)) { (regNodeTx, height) =>
      val blockchain: Blockchain = mock[Blockchain]
      (blockchain.participantPubKey _).expects(regNodeTx.target.toAddress).returning(None).anyNumberOfTimes()

      RegisterNodeTransactionDiff(blockchain, height)(regNodeTx) should produce(
        s"Account '${regNodeTx.target}' is not present in blockchain, therefore not allowed to remove it")
    }
  }

  property("Doesn't allow to remove node if participant count will be less then minimum") {
    forAll(
      registerNodeTransactionGen(Gen.const(OpType.Remove)),
      Gen.chooseNum(2, Int.MaxValue),
      severalAddressGenerator(1, minNetworkParticipantsCount)
    ) { (regNodeTx, height, participants) =>
      val blockchain: Blockchain = mock[Blockchain]
      (blockchain.networkParticipants _).expects().returning(participants).once()
      (blockchain.participantPubKey _).expects(regNodeTx.target.toAddress).returning(Some(regNodeTx.target)).once()

      RegisterNodeTransactionDiff(blockchain, height)(regNodeTx) should produce(
        s"Account '${regNodeTx.target}' cannot be removed because number of participants will be less then '${minNetworkParticipantsCount}'")
    }
  }

  property("Produced Diffs combination is right") {
    forAll(registerNodeTransactionGen(),
           Gen.chooseNum(2, Int.MaxValue),
           severalAddressGenerator(minNetworkParticipantsCount + 1, minNetworkParticipantsCount + 10)) { (regNodeTx, height, participants) =>
      val blockchain: Blockchain = mock[Blockchain]
      (blockchain.participantPubKey _).expects(regNodeTx.target.toAddress).returning(None).once()

      val addNodeTx    = regNodeTx.copy(opType = OpType.Add)
      val afterAddDiff = RegisterNodeTransactionDiff(blockchain, height)(addNodeTx).explicitGet()

      (blockchain.networkParticipants _).expects().returning(participants).once()
      (blockchain.participantPubKey _).expects(regNodeTx.target.toAddress).returning(Some(regNodeTx.target)).once()
      val removeNodeTx    = regNodeTx.copy(opType = OpType.Remove)
      val afterRemoveDiff = RegisterNodeTransactionDiff(blockchain, height)(removeNodeTx).explicitGet()

      val resultDiff = afterAddDiff.combine(afterRemoveDiff)
      resultDiff.registrations shouldBe empty
    }
  }

  property("Add, Remove, Add in neighboring blocks should work") {
    val preconditions: Gen[(Block, RegisterNodeTransactionV1, RegisterNodeTransactionV1, RegisterNodeTransactionV1)] = for {
      genesisTime        <- ntpTimestampGen.map(_ - 1.minute.toMillis)
      sender             <- accountGen
      target             <- accountGen
      firstAddTx         <- registerNodeTransactionGen(Gen.const(sender), Gen.const(target), OpType.Add)
      removeTx           <- registerNodeTransactionGen(Gen.const(sender), Gen.const(target), OpType.Remove)
      secondAddTx        <- registerNodeTransactionGen(Gen.const(sender), Gen.const(target), OpType.Add)
      genesisRegisterTxs <- Gen.listOfN(3, genesisRegisterNodeTxGen(Gen.const(genesisTime)))
      genesisForSender = GenesisTransaction.create(sender.toAddress, ENOUGH_AMT, genesisTime).explicitGet()
      permitForSender <- genesisPermitTxGen(sender.toAddress, Role.ConnectionManager, genesisTime - 110)
      genesisBlock = TestBlock.create(genesisTime, Seq(genesisForSender, permitForSender) ++ genesisRegisterTxs)
    } yield (genesisBlock, firstAddTx, removeTx, secondAddTx)
    forAll(preconditions) {
      case (genesisBlock, firstAddTx, removeTx, secondAddTx) =>
        withDomain(MicroblocksActivatedAt0WESettings) { domain =>
          val bcu                = domain.blockchainUpdater
          val blockTimestampDiff = 1.minute.toMillis
          bcu.processBlock(genesisBlock, ConsensusPostAction.NoAction).explicitGet()
          val firstAddTxBlock = TestBlock.create(genesisBlock.timestamp + blockTimestampDiff, genesisBlock.signerData.signature, Seq(firstAddTx))
          bcu.processBlock(firstAddTxBlock, ConsensusPostAction.NoAction).explicitGet() shouldBe Some(ListBuffer.empty)
          bcu.participantPubKey(firstAddTx.target.toAddress) shouldBe Some(firstAddTx.target)
          val removeTxBlock = TestBlock.create(firstAddTxBlock.timestamp + blockTimestampDiff, firstAddTxBlock.signerData.signature, Seq(removeTx))
          bcu.processBlock(removeTxBlock, ConsensusPostAction.NoAction).explicitGet() shouldBe Some(ListBuffer.empty)
          bcu.participantPubKey(firstAddTx.target.toAddress) shouldBe None
          val secondAddTxBlock = TestBlock.create(removeTxBlock.timestamp + blockTimestampDiff, removeTxBlock.signerData.signature, Seq(secondAddTx))
          bcu.processBlock(secondAddTxBlock, ConsensusPostAction.NoAction).explicitGet() shouldBe Some(ListBuffer.empty)
          bcu.participantPubKey(firstAddTx.target.toAddress) shouldBe Some(secondAddTx.target)
        }
    }
  }
}
