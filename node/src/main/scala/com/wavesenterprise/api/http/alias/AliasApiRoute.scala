package com.wavesenterprise.api.http.alias

import akka.http.scaladsl.server.Route
import com.wavesenterprise.account.{Address, Alias}
import com.wavesenterprise.api.http.ApiError.AliasDoesNotExist
import com.wavesenterprise.api.http._
import com.wavesenterprise.settings.ApiSettings
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.utils.Time
import com.wavesenterprise.utx.UtxPool
import monix.execution.schedulers.SchedulerService
import play.api.libs.json.{Format, Json}

class AliasApiRoute(val settings: ApiSettings,
                    val utx: UtxPool,
                    val time: Time,
                    blockchain: Blockchain,
                    val nodeOwner: Address,
                    val scheduler: SchedulerService)
    extends ApiRoute {

  override val route = pathPrefix("alias") {
    withAuth() {
      addressOfAlias ~ aliasOfAddress
    }
  }

  /**
    * GET /alias/by-alias/{alias}
    *
    * Returns an address associated with an Alias. Alias should be plain text without an 'alias' prefix and network code.
    **/
  def addressOfAlias: Route = (get & path("by-alias" / Segment)) { aliasName =>
    withExecutionContext(scheduler) {
      val result = Alias.buildWithCurrentChainId(aliasName) match {
        case Right(alias) =>
          blockchain.resolveAlias(alias) match {
            case Right(addr) => Right(Address(addr.stringRepr))
            case _           => Left(AliasDoesNotExist(alias))
          }
        case Left(err) => Left(ApiError.fromCryptoError(err))
      }
      complete(result)
    }
  }

  /**
    * GET /alias/by-address/{address}
    *
    * Returns a collection of aliases associated with an address
    **/
  def aliasOfAddress: Route = (get & path("by-address" / Segment)) { addressString =>
    withExecutionContext(scheduler) {
      val result: Either[ApiError, Seq[String]] = com.wavesenterprise.account.Address
        .fromString(addressString)
        .map(acc => blockchain.aliasesIssuedByAddress(acc).map(_.stringRepr).toSeq)
        .left
        .map(ApiError.fromCryptoError)
      complete(result)
    }
  }

  case class Address(address: String)

  implicit val addressFormat: Format[Address] = Json.format
}
