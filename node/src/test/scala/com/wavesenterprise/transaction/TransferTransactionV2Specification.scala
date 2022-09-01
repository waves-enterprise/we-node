package com.wavesenterprise.transaction

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.api.http.assets.TransferV2Request
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.scalatest._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class TransferTransactionV2Specification
    extends AnyPropSpec
    with ScalaCheckPropertyChecks
    with Matchers
    with TransactionGen
    with WithSenderAndRecipient {

  property("VersionedTransferTransactionSpecification negative validation cases") {
    forAll(transferV2Gen) {
      case TransferTransactionV2(sender, assetId, feeAssetId, timestamp, amount, fee, recipient, _, _) =>
        val invalidAttachment = "as.dsfsadfsd"
        val invalidAttachmentRequest = TransferV2Request(
          version = 2,
          assetId = assetId.map(_.toString),
          amount = amount,
          feeAssetId = feeAssetId.map(_.toString),
          fee = fee,
          sender = sender.toString,
          attachment = Some(invalidAttachment),
          recipient = recipient.toString,
          timestamp = Some(timestamp),
          password = None
        )
        val invalidAttachmentEither = TransactionFactory.transferAssetV2(invalidAttachmentRequest, sender)
        invalidAttachmentEither shouldBe Left(ValidationError.GenericError(s"Invalid Base58 attachment string '$invalidAttachment'"))
    }
  }

  property("Empty string for feeAssetId in a request must be treated as a None") {
    forAll(transferV2Gen) {
      case TransferTransactionV2(sender, _, _, timestamp, amount, fee, recipient, _, _) =>
        val requestWithNoneFeeAssetId =
          TransferV2Request(2, None, amount, None, fee, sender.toString, None, recipient.toString, Some(timestamp), None)
        val requestWithEmptyFeeAssetId = requestWithNoneFeeAssetId.copy(feeAssetId = Some(""))

        val tx1 = TransactionFactory.transferAssetV2(requestWithNoneFeeAssetId, sender).explicitGet()
        val tx2 = TransactionFactory.transferAssetV2(requestWithEmptyFeeAssetId, sender).explicitGet()

        tx1.bodyBytes() should contain theSameElementsAs (tx2.bodyBytes())
    }
  }
}
