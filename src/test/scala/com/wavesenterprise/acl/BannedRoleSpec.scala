package com.wavesenterprise.acl

import com.wavesenterprise.block.Block
import com.wavesenterprise.crypto.SignatureLength
import com.wavesenterprise.db.WithDomain
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.TestFees
import com.wavesenterprise.state._
import com.wavesenterprise.state.diffs.TransactionDiffer.TransactionValidationError
import com.wavesenterprise.transaction.ValidationError.PermissionError
import com.wavesenterprise.transaction.acl.{PermitTransaction, PermitTransactionV1}
import com.wavesenterprise.transaction.transfer.{TransferTransaction, TransferTransactionV2}
import com.wavesenterprise.transaction.{GenesisPermitTransaction, GenesisTransaction, ValidationError}
import com.wavesenterprise.utils.NumberUtils.DoubleExt
import com.wavesenterprise.{TestTime, TransactionGen}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.scalatest.{Assertion, FreeSpec, Matchers}

class BannedRoleSpec extends FreeSpec with Matchers with WithDomain with TransactionGen {
  private val transferFee: Long    = TestFees.defaultFees.forTxType(TransferTransaction.typeId)
  private val permitFee: Long      = TestFees.defaultFees.forTxType(PermitTransaction.typeId)
  private val initialBalance: Long = 10000.west
  private val time                 = new TestTime
  private def nextTs: Long         = time.getTimestamp()

  private val adminNode  = accountGen.sample.get
  private val bannedNode = accountGen.sample.get
  private val anyNode    = accountGen.sample.get

  private val genesisBlock: Block = {
    val genesisTs = nextTs
    val permitTx  = GenesisPermitTransaction.create(adminNode.toAddress, Role.Blacklister, genesisTs + 1).explicitGet()

    val addresses  = List(adminNode.toAddress, bannedNode.toAddress, anyNode.toAddress)
    val genesisTxs = addresses.map(GenesisTransaction.create(_, initialBalance, genesisTs).explicitGet())
    TestBlock.create(
      genesisTs,
      ByteStr(Array.fill[Byte](SignatureLength)(0)),
      genesisTxs :+ permitTx
    )
  }

  private def checkValidationErrorMessage(appendResult: Either[ValidationError, _], message: String): Assertion = {
    appendResult.left.get.asInstanceOf[TransactionValidationError].cause.asInstanceOf[PermissionError].err should include(message)
  }

  "reject transactions from banned node" in {
    withDomain() { d =>
      d.appendBlock(genesisBlock)

      val banTxTimestamp = nextTs
      val banTransaction = PermitTransactionV1
        .selfSigned(adminNode, bannedNode.toAddress, banTxTimestamp, permitFee, PermissionOp(OpType.Add, Role.Banned, banTxTimestamp, None))
        .explicitGet()
      val banBlock = TestBlock.create(banTxTimestamp, genesisBlock.signerData.signature, Seq(banTransaction), adminNode)
      d.appendBlock(banBlock)

      val forbiddenTxTimestamp = nextTs
      val forbiddenTransaction =
        TransferTransactionV2.selfSigned(bannedNode, None, None, forbiddenTxTimestamp, 10, transferFee, anyNode.toAddress, Array.empty).explicitGet()

      val appendResult =
        d.tryAppendBlock(TestBlock.create(forbiddenTxTimestamp, banBlock.signerData.signature, Seq(forbiddenTransaction), bannedNode))
      appendResult shouldBe 'left
      checkValidationErrorMessage(appendResult, "Sender is 'banned'")
    }
  }

  "transactions allowed after banned role is removed" in {
    withDomain() { d =>
      d.appendBlock(genesisBlock)

      val banTxTimestamp = nextTs
      val banTransaction = PermitTransactionV1
        .selfSigned(adminNode, bannedNode.toAddress, banTxTimestamp, permitFee, PermissionOp(OpType.Add, Role.Banned, banTxTimestamp, None))
        .explicitGet()
      val banBlock = TestBlock.create(banTxTimestamp, genesisBlock.signerData.signature, Seq(banTransaction), adminNode)
      d.appendBlock(banBlock)

      val removeBanTxTimestamp = nextTs
      val removeBanTransaction = PermitTransactionV1
        .selfSigned(adminNode,
                    bannedNode.toAddress,
                    removeBanTxTimestamp,
                    permitFee,
                    PermissionOp(OpType.Remove, Role.Banned, removeBanTxTimestamp, None))
        .explicitGet()
      val removeBanBlock = TestBlock.create(removeBanTxTimestamp, banBlock.signerData.signature, Seq(removeBanTransaction), adminNode)
      d.appendBlock(removeBanBlock)

      val forbiddenTxTimestamp = nextTs
      val forbiddenTransaction =
        TransferTransactionV2.selfSigned(bannedNode, None, None, forbiddenTxTimestamp, 10, transferFee, anyNode.toAddress, Array.empty).explicitGet()

      val appendResult =
        d.tryAppendBlock(TestBlock.create(forbiddenTxTimestamp, removeBanBlock.signerData.signature, Seq(forbiddenTransaction), bannedNode))
      appendResult shouldBe 'right
    }
  }

  "non-blacklister cannot add banned role" in {
    withDomain() { d =>
      d.appendBlock(genesisBlock)

      val banTxTimestamp = nextTs
      val banTransaction = PermitTransactionV1
        .selfSigned(anyNode, bannedNode.toAddress, banTxTimestamp, permitFee, PermissionOp(OpType.Add, Role.Banned, banTxTimestamp, None))
        .explicitGet()
      val banBlock     = TestBlock.create(banTxTimestamp, genesisBlock.signerData.signature, Seq(banTransaction), anyNode)
      val appendResult = d.tryAppendBlock(banBlock)
      checkValidationErrorMessage(appendResult, "Doesn't have any of required roles: permissioner, blacklister")
    }
  }

}
