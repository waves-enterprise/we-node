package com.wavesenterprise.api.http.leasing

import akka.http.scaladsl.server.Route
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http._
import com.wavesenterprise.settings.ApiSettings
import com.wavesenterprise.state.{Blockchain, LeaseId}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.lease.LeaseTransaction
import com.wavesenterprise.utils.Time
import com.wavesenterprise.utx.UtxPool
import com.wavesenterprise.wallet.Wallet
import monix.execution.schedulers.SchedulerService
import play.api.libs.json.JsNumber

class LeaseApiRoute(val settings: ApiSettings,
                    wallet: Wallet,
                    blockchain: Blockchain,
                    val utx: UtxPool,
                    val time: Time,
                    val nodeOwner: Address,
                    val scheduler: SchedulerService)
    extends ApiRoute {

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
          case Left(e) => ApiError.fromCryptoError(e)
          case Right(a) =>
            blockchain
              .addressTransactions(a, Set(LeaseTransaction.typeId), Int.MaxValue, None)
              .explicitGet()
              .collect {
                case (h, lt: LeaseTransaction) if blockchain.leaseDetails(LeaseId(lt.id())).exists(_.isActive) =>
                  lt.json() + ("height" -> JsNumber(h))
              }
        })
      }
    }
  }
}
