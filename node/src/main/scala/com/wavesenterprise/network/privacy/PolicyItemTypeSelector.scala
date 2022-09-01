package com.wavesenterprise.network.privacy

import cats.implicits._
import cats.data.EitherT
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.database.PrivacyState
import com.wavesenterprise.privacy.{PolicyDataHash, PolicyStorage, PrivacyDataType, PrivacyItemDescriptor}
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Task

trait PolicyItemTypeSelector extends ScorexLogging {
  import PolicyItemTypeSelector._

  protected def state: Blockchain with PrivacyState
  protected def storage: PolicyStorage

  /**
    * Determines the type of policy item and handles the case when the RocksDB storage does not contain item descriptor
    * (for example, as a result of replacing state from another node).
    */
  def policyItemType(policyId: ByteStr, dataHash: PolicyDataHash): Task[PrivacyDataType] = Task.defer {
    state.privacyItemDescriptor(policyId, dataHash) match {
      case Some(PrivacyItemDescriptor(foundType)) =>
        Task.pure(foundType)
      case None =>
        EitherT(storage.policyItemType(policyId.toString, dataHash.toString))
          .semiflatMap {
            case None =>
              Task.raiseError(PolicyItemTypeSelectorError.ItemNotFoundError(policyId, dataHash))
            case Some(determinedType) =>
              Task {
                log.debug(s"Policy '$policyId' data '$dataHash' type successfully determined: $determinedType, updating state")
                state.putItemDescriptor(policyId, dataHash, PrivacyItemDescriptor(determinedType))
              }.as(determinedType)
          } valueOrF { err =>
          Task.raiseError(PolicyItemTypeSelectorError.StorageUnavailableError(policyId, dataHash, err))
        }

    }
  }
}

object PolicyItemTypeSelector {
  sealed abstract class PolicyItemTypeSelectorError(val message: String) extends RuntimeException(message)

  object PolicyItemTypeSelectorError {
    case class ItemNotFoundError(policyId: ByteStr, dataHash: PolicyDataHash)
        extends PolicyItemTypeSelectorError(s"Failed to determine policy '$policyId' data '$dataHash' type: item not found")
    case class StorageUnavailableError(policyId: ByteStr, dataHash: PolicyDataHash, error: ApiError)
        extends PolicyItemTypeSelectorError(s"Failed to determine policy '$policyId' data '$dataHash' type: $error")
  }
}
