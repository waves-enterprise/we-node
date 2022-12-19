package com.wavesenterprise.lagonaki.unit

import com.wavesenterprise.block.Block
import com.wavesenterprise.consensus.PoSLikeConsensusBlockData
import com.wavesenterprise.state._
import com.wavesenterprise.state.diffs.produce
import com.wavesenterprise.transaction._
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalatest._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class BlockSpecification extends AnyPropSpec with ScalaCheckPropertyChecks with TransactionGen with Matchers with NoShrink {

  private val time = System.currentTimeMillis() - 5000

  private val blockGen = for {
    baseTarget          <- arbitrary[Long]
    reference           <- byteArrayGen(Block.BlockIdLength).map(r => ByteStr(r))
    generationSignature <- byteArrayGen(Block.GeneratorSignatureLength)
    assetBytes          <- byteArrayGen(AssetIdLength)
    assetId = Some(ByteStr(assetBytes))
    sender                    <- accountGen
    recipient                 <- accountGen
    paymentTransaction        <- westTransferGeneratorP(time, sender, recipient.toAddress)
    transferTrancation        <- transferGeneratorP(1 + time, sender, recipient.toAddress, assetId, None)
    anotherPaymentTransaction <- westTransferGeneratorP(2 + time, sender, recipient.toAddress)
    transactionData = Seq(paymentTransaction, transferTrancation, anotherPaymentTransaction)
  } yield (baseTarget, reference, ByteStr(generationSignature), recipient, transactionData)

  def bigBlockGen(amt: Int): Gen[Block] =
    for {
      baseTarget          <- arbitrary[Long]
      reference           <- byteArrayGen(Block.BlockIdLength).map(r => ByteStr(r))
      generationSignature <- byteArrayGen(Block.GeneratorSignatureLength)
      sender              <- accountGen
      recipient           <- accountGen
      paymentTransaction  <- westTransferGeneratorP(time, sender, recipient.toAddress)
    } yield Block
      .buildAndSign(3,
                    time,
                    reference,
                    PoSLikeConsensusBlockData(baseTarget, ByteStr(generationSignature)),
                    Seq.fill(amt)(paymentTransaction),
                    recipient,
                    Set.empty)
      .explicitGet()

  property(" block with txs bytes/parse roundtrip version 1,2") {
    Seq[Byte](1, 2).foreach { version =>
      forAll(blockGen) {
        case (baseTarget, reference, generationSignature, recipient, transactionData) =>
          val block = Block
            .buildAndSign(version, time, reference, PoSLikeConsensusBlockData(baseTarget, generationSignature), transactionData, recipient, Set.empty)
            .explicitGet()
          val parsedBlock = Block.parseBytes(block.bytes()).get
          assert(Signed.validate(block).isRight)
          assert(Signed.validate(parsedBlock).isRight)
          assert(parsedBlock.consensusData.asPoSMaybe().fold(_ => fail(s"Pos expected, but got Poa"), _.generationSignature == generationSignature))
          assert(parsedBlock.version.toInt == version)
          assert(
            parsedBlock.signerData.generator.publicKey.getEncoded
              .sameElements(recipient.publicKey.getEncoded))
      }
    }
  }

  property(" block version 1,2 could not contain feature votes") {
    Seq[Byte](1, 2).foreach { version =>
      forAll(blockGen) {
        case (baseTarget, reference, generationSignature, recipient, transactionData) =>
          Block.buildAndSign(version,
                             time,
                             reference,
                             PoSLikeConsensusBlockData(baseTarget, generationSignature),
                             transactionData,
                             recipient,
                             Set(1)) should produce(
            "could not contain feature votes")
      }
    }
  }

  property(s" feature flags limit is ${Block.MaxFeaturesInBlock}") {
    val version           = 3.toByte
    val supportedFeatures = (0 to Block.MaxFeaturesInBlock * 2).map(_.toShort).toSet

    forAll(blockGen) {
      case (baseTarget, reference, generationSignature, recipient, transactionData) =>
        Block.buildAndSign(version,
                           time,
                           reference,
                           PoSLikeConsensusBlockData(baseTarget, generationSignature),
                           transactionData,
                           recipient,
                           supportedFeatures) should produce(s"Block could not contain more than '${Block.MaxFeaturesInBlock}' feature votes")
    }
  }
  property(" block with txs bytes/parse roundtrip version 3") {
    val version = 3.toByte

    val faetureSetGen: Gen[Set[Short]] = Gen.choose(0, Block.MaxFeaturesInBlock).flatMap(fc => Gen.listOfN(fc, arbitrary[Short])).map(_.toSet)

    forAll(blockGen, faetureSetGen) {
      case ((baseTarget, reference, generationSignature, recipient, transactionData), featureVotes) =>
        val block = Block
          .buildAndSign(version,
                        time,
                        reference,
                        PoSLikeConsensusBlockData(baseTarget, generationSignature),
                        transactionData,
                        recipient,
                        featureVotes)
          .explicitGet()
        val parsedBlock = Block.parseBytes(block.bytes()).get
        assert(Signed.validate(block).isRight)
        assert(Signed.validate(parsedBlock).isRight)
        assert(parsedBlock.consensusData.asPoSMaybe().fold(_ => fail(s"Pos expected, but got Poa"), _.generationSignature == generationSignature))
        assert(parsedBlock.version.toInt == version)
        assert(
          parsedBlock.signerData.generator.publicKey.getEncoded
            .sameElements(recipient.publicKey.getEncoded))
        assert(parsedBlock.featureVotes == featureVotes)
    }
  }
}
