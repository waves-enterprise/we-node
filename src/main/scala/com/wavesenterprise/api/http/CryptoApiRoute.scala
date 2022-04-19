package com.wavesenterprise.api.http

import akka.http.scaladsl.server.Route
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.auth.WithAuthFromContract
import com.wavesenterprise.api.http.service.CryptoApiService
import com.wavesenterprise.docker.ContractAuthTokenService
import com.wavesenterprise.settings.ApiSettings
import com.wavesenterprise.utils.Time
import monix.execution.schedulers.SchedulerService

import scala.concurrent.duration._

object CryptoApiRoute {
  final val encryptEntityMaxSize: Long = 165 * 1024 // 165 Kb
}

class CryptoApiRoute(cryptoService: CryptoApiService,
                     val settings: ApiSettings,
                     val time: Time,
                     val contractAuthTokenService: Option[ContractAuthTokenService],
                     val nodeOwner: Address,
                     val scheduler: SchedulerService)
    extends ApiRoute
    with WithAuthFromContract {

  override val route: Route = pathPrefix("crypto") {
    (withContractAuth | withAuth()) {
      encryptSeparate ~ encryptCommon ~ decryptData
    }
  }

  /**
    * POST /crypto/encryptSeparate
    *
    * Encrypt text for recipients separately
    **/
  def encryptSeparate: Route = withRequestTimeout(1.minute) {
    withSizeLimit(CryptoApiRoute.encryptEntityMaxSize) {
      withExecutionContext(scheduler) {
        (pathPrefix("encryptSeparate") & post) {
          json[EncryptDataRequest](cryptoService.encryptSeparate)
        }
      }
    }
  }

  /**
    * POST /crypto/encryptCommon
    *
    * Encrypt text for recipients on a common key
    **/
  def encryptCommon: Route = withRequestTimeout(1.minute) {
    withSizeLimit(CryptoApiRoute.encryptEntityMaxSize) {
      withExecutionContext(scheduler) {
        (pathPrefix("encryptCommon") & post) {
          json[EncryptDataRequest](cryptoService.encryptCommon)
        }
      }
    }
  }

  /**
    * POST /crypto/decrypt
    *
    * Decrypt text by private key
    **/
  def decryptData: Route = (pathPrefix("decrypt") & post) {
    withExecutionContext(scheduler) {
      json[DecryptDataRequest](cryptoService.decrypt)
    }
  }
}
