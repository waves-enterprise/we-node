package com.wavesenterprise.api.http.leasing

import akka.http.scaladsl.server.Route
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http._
import com.wavesenterprise.settings.ApiSettings
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.transaction.docker.ExecutedContractTransactionV3
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation.{ContractCancelLeaseV1, ContractLeaseV1}
import com.wavesenterprise.state.{Blockchain, LeaseId}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.lease.LeaseTransaction
import com.wavesenterprise.utils.Time
import com.wavesenterprise.utx.UtxPool
import com.wavesenterprise.wallet.Wallet
import monix.execution.schedulers.SchedulerService
import play.api.libs.json.{JsNumber, JsObject}

import scala.collection.mutable

class LeaseApiRoute(
    val settings: ApiSettings,
    wallet: Wallet,
    blockchain: Blockchain,
    val utx: UtxPool,
    val time: Time,
    val nodeOwner: Address,
    val scheduler: SchedulerService
) extends ApiRoute {

  override val route = pathPrefix("leasing") {
    withAuth() {
      active
    }
  }

  /**
    * GET /leasing/active/{address}
    **/
  def active: Route = (pathPrefix("active") & get) {
    pathPrefix(Segment) { address =>
      withExecutionContext(scheduler) {
        complete(Address.fromString(address) match {
          case Left(error) => ApiError.fromCryptoError(error)
          case Right(address) =>
            val leaseTxs = blockchain
              .addressTransactions(address, Set(LeaseTransaction.typeId), Int.MaxValue, None)
              .explicitGet()
              .collect {
                case (height, leaseTx: LeaseTransaction) if blockchain.leaseDetails(LeaseId(leaseTx.id())).exists(_.isActive) =>
                  leaseTx.json() + ("height" -> JsNumber(height))
              }

            val assetOperationLeaseTxs = findAssetOperationLease(address)

            leaseTxs ++ assetOperationLeaseTxs
        })
      }
    }
  }

  private def findAssetOperationLease(address: Address): Seq[JsObject] = {
    val leaseOps       = mutable.Map[ByteStr, JsObject]()
    val leaseCancelOps = mutable.Set[ByteStr]()

    blockchain
      .addressTransactions(address, Set(ExecutedContractTransactionV3.typeId), Int.MaxValue, None)
      .explicitGet()
      .foreach {
        case (height, tx: ExecutedContractTransactionV3) =>
          tx.assetOperations.foreach {
            case leaseOp: ContractLeaseV1 =>
              val txJson = tx.json() + ("height" -> JsNumber(height))
              leaseOps.put(leaseOp.leaseId, txJson)

            case leaseCancelOp: ContractCancelLeaseV1 =>
              leaseCancelOps.add(leaseCancelOp.leaseId)

            case _ => ()
          }

        case _ => ()
      }

    leaseCancelOps.foreach(leaseOps.remove)

    leaseOps.values.toSeq
  }
}
