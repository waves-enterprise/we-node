package com.wavesenterprise.api.http

import akka.http.scaladsl.server.Route
import cats.implicits._
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.ApiError.ScriptCompilerError
import com.wavesenterprise.api.http.auth.ApiProtectionLevel.ApiKeyProtection
import com.wavesenterprise.api.http.auth.AuthRole.Administrator
import com.wavesenterprise.crypto
import com.wavesenterprise.settings.ApiSettings
import com.wavesenterprise.transaction.smart.script.{Script, ScriptCompiler}
import com.wavesenterprise.utils.{Base58, Time}
import com.wavesenterprise.wallet.Wallet
import monix.execution.schedulers.SchedulerService
import play.api.libs.json.{Format, Json}

class UtilsApiRoute(timeService: Time, val settings: ApiSettings, wallet: Wallet, val time: Time, val nodeOwner: Address, scheduler: SchedulerService)
    extends ApiRoute
    with AdditionalDirectiveOps {

  import UtilsApiRoute._

  protected def adminAuth = withAuth(ApiKeyProtection, Administrator)

  override val route: Route = pathPrefix("utils") {
    reloadWallet ~ withAuth() {
      compile ~ estimate ~ getNodeTime ~ hashFast ~ hashSecure
    }
  }

  /**
    * POST /utils/script/compile
    *
    * Compiles RIDE code to Base64 script representation
    **/
  def compile: Route = path("script" / "compile") {
    (post & entity(as[String])) { code =>
      parameter('assetScript.as[Boolean] ? false) { isAssetScript =>
        complete(
          ScriptCompiler(code, isAssetScript).fold(
            e => ScriptCompilerError(e),
            {
              case (script, complexity) =>
                CompilationResult(script.bytes().base64, complexity)
            }
          )
        )
      }
    }
  }

  /**
    * POST /utils/script/estimate
    *
    * Estimates RIDE compiled code in Base64 representation
    **/
  def estimate: Route = path("script" / "estimate") {
    (post & entity(as[String])) { code =>
      complete(
        for {
          _ <- Either.cond(code.nonEmpty, (), ScriptCompilerError("Cannot estimate empty script: expected compiled script in Base64, got nothing"))
          script <- Script
            .fromBase64String(code)
            .leftMap(parseError => ScriptCompilerError(parseError.message))
          complexity <- ScriptCompiler
            .estimate(script, script.version)
            .leftMap(ScriptCompilerError.apply)
        } yield EstimationResult(code, complexity, script.text)
      )
    }
  }

  /**
    * GET /utils/time
    *
    * Current Node time (UTC)
    **/
  def getNodeTime: Route = (path("time") & get) {
    complete(TimeResult(System.currentTimeMillis(), timeService.correctedTime()))
  }

  /**
    * POST /utils/hash/secure
    *
    * Return SecureCryptographicHash of specified message
    **/
  def hashSecure: Route = (path("hash" / "secure") & post & addedGuard) {
    entity(as[String]) { message =>
      complete(HashingResult(message, Base58.encode(crypto.secureHash(message))))
    }
  }

  /**
    * POST /utils/hash/fast
    *
    * Return FastCryptographicHash of specified message
    **/
  def hashFast: Route = (path("hash" / "fast") & post & addedGuard) {
    entity(as[String]) { message =>
      complete(HashingResult(message, Base58.encode(crypto.fastHash(message))))
    }
  }

  /**
    * POST /utils/reload-wallet
    *
    * Reloads wallet after its update
    **/
  def reloadWallet: Route = (path("reload-wallet") & post & adminAuth) {
    withExecutionContext(scheduler) {
      wallet.reload()
      complete(Json.obj("message" -> "Wallet reloaded successfully"))
    }
  }
}

object UtilsApiRoute {
  val DefaultSeedSize = 32

  trait ScriptResult {
    val script: String
    val complexity: Long
  }

  case class CompilationResult(script: String, complexity: Long) extends ScriptResult

  case class EstimationResult(script: String, complexity: Long, scriptText: String) extends ScriptResult

  case class HashingResult(message: String, hash: String)

  case class TimeResult(system: Long, NTP: Long)

  implicit val compilationResultFormat: Format[CompilationResult] = Json.format

  implicit val estimationResultFormat: Format[EstimationResult] = Json.format

  implicit val hashingResultFormat: Format[HashingResult] = Json.format

  implicit val timeResultFormat: Format[TimeResult] = Json.format
}
