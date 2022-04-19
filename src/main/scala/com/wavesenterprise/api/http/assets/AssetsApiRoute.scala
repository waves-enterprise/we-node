package com.wavesenterprise.api.http.assets

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.server.Route
import cats.instances.either.catsStdInstancesForEither
import cats.instances.option.catsStdInstancesForOption
import cats.syntax.either._
import cats.syntax.traverse._
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.ApiError.{CustomValidationError, TooBigArrayAllocation}
import com.wavesenterprise.api.http.{ApiError, _}
import com.wavesenterprise.settings.ApiSettings
import com.wavesenterprise.state.{Blockchain, ByteStr, dstPageWrites, dstWrites}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.smart.script.ScriptCompiler
import com.wavesenterprise.utils.{Base58, Time}
import com.wavesenterprise.utx.UtxPool
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService
import play.api.libs.json._

import java.util.concurrent._
import scala.concurrent.Future

class AssetsApiRoute(val settings: ApiSettings,
                     val utx: UtxPool,
                     blockchain: Blockchain,
                     val time: Time,
                     val nodeOwner: Address,
                     val scheduler: SchedulerService)
    extends ApiRoute {

  import AssetsApiRoute._

  private val distributionTaskScheduler = {
    val executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable](AssetsApiRoute.MAX_DISTRIBUTION_TASKS))
    Scheduler(executor)
  }

  override lazy val route: Route =
    pathPrefix("assets") {
      withAuth() {
        balance ~ balances ~ addressesBalances ~ balanceDistributionAtHeight ~ balanceDistribution ~ details
      }
    }

  /**
    * GET /assets/balance/{address}/{assetId}
    *
    * Account's balance by given asset
    **/
  def balance: Route = (get & path("balance" / Segment / Segment)) { (address, assetId) =>
    withExecutionContext(scheduler) {
      complete(balanceJson(address, assetId))
    }
  }

  /**
    * GET /assets/{assetId}/distribution/limit/{limit}
    *
    * Asset balance distribution by account
    **/
  def balanceDistribution: Route = (get & path(Segment / "distribution")) { (assetParam) =>
    withExecutionContext(scheduler) {
      val assetIdEi = AssetsApiRoute
        .validateAssetId(assetParam)

      val distributionTask = assetIdEi match {
        case Left(err) => Task.pure(ApiError.fromValidationError(err): ToResponseMarshallable)
        case Right(assetId) =>
          Task
            .eval(blockchain.assetDistribution(assetId))
            .map(dst => Json.toJson(dst)(dstWrites): ToResponseMarshallable)
      }

      complete {
        try {
          distributionTask.runAsyncLogErr(distributionTaskScheduler)
        } catch {
          case _: RejectedExecutionException =>
            val errMsg = CustomValidationError("Asset distribution currently unavailable, try again later")
            Future.successful(errMsg.json: ToResponseMarshallable)
        }
      }
    }
  }

  /**
    * GET /assets/{assetId}/distribution/{height}/limit/{limit}
    *
    * Asset balance distribution by account at specified height
    **/
  def balanceDistributionAtHeight: Route =
    (get & path(Segment / "distribution" / IntNumber / "limit" / IntNumber) & parameter('after.?)) {
      (assetParam, heightParam, limitParam, afterParam) =>
        withExecutionContext(scheduler) {
          val paramsEi: Either[ValidationError, DistributionParams] =
            AssetsApiRoute
              .validateDistributionParams(blockchain, assetParam, heightParam, limitParam, settings.rest.distributionAddressLimit, afterParam)

          val resultTask = paramsEi match {
            case Left(err)     => Task.pure(ApiError.fromValidationError(err): ToResponseMarshallable)
            case Right(params) => assetDistributionTask(params)
          }

          complete {
            try {
              resultTask.runAsyncLogErr(distributionTaskScheduler)
            } catch {
              case _: RejectedExecutionException =>
                val errMsg = CustomValidationError("Asset distribution currently unavailable, try again later")
                Future.successful(errMsg.json: ToResponseMarshallable)
            }
          }
        }
    }

  /**
    * GET /assets/balance/{address}
    *
    * Account's balances for all assets
    **/
  def balances: Route = (get & path("balance" / Segment)) { address =>
    withExecutionContext(scheduler) {
      complete(fullAccountAssetsInfo(address))
    }
  }

  /**
    * POST /assets/balance
    *
    * Assets balances for few addresses
    **/
  def addressesBalances: Route = (path("balance") & post) {
    withExecutionContext(scheduler) {
      json[AddressesMessage](message => {
        if (message.addresses.length <= MaxAddressesPerRequest) {
          val addresses = message.addresses.map(Address.fromString)
          val invalid   = addresses.find(_.isLeft)
          invalid match {
            case Some(value) => ApiError.fromCryptoError(value.left.get)
            case None =>
              val result = addresses.map(_.right.get).map { address =>
                val balances = blockchain
                  .portfolio(address)
                  .assets
                  .view
                  .filter(_._2 > 0)
                  .map {
                    case (assetId, balance) =>
                      Json.obj("assetId" -> assetId.toString, "balance" -> JsNumber(BigDecimal(balance)))
                  }
                  .toArray
                (address.address, JsArray(balances))
              }
              JsObject(result)
          }
        } else {
          TooBigArrayAllocation
        }
      })
    }
  }

  /**
    * GET /assets/details/{assetId}
    *
    * Provides detailed information about given asset
    **/
  def details: Route = (get & path("details" / Segment)) { id =>
    withExecutionContext(scheduler) {
      parameters('full.as[Boolean].?) { full =>
        complete(assetDetails(id, full.getOrElse(false)))
      }
    }
  }

  private def balanceJson(addressStr: String, assetIdStr: String): Either[ValidationError, JsObject] = {
    for {
      assetId <- ByteStr
        .decodeBase58(assetIdStr)
        .toEither
        .leftMap(ex => ValidationError.InvalidAddress(s"Asset id '$assetIdStr' is invalid: ${ex.getMessage}"))
      address <- Address.fromString(addressStr).leftMap(ValidationError.fromCryptoError)
    } yield {
      val balance = blockchain.balance(address, Some(assetId))
      Json.obj("address" -> address.address, "assetId" -> assetIdStr, "balance" -> JsNumber(BigDecimal(balance)))
    }
  }

  def assetDistributionTask(params: DistributionParams): Task[ToResponseMarshallable] = {
    val (assetId, height, limit, maybeAfter) = params

    val distributionTask = Task.eval(
      blockchain
        .assetDistributionAtHeight(assetId, height, limit, maybeAfter)
    )

    distributionTask.map {
      case Right(dst) => Json.toJson(dst)(dstPageWrites): ToResponseMarshallable
      case Left(err)  => ApiError.fromValidationError(err)
    }
  }

  private def fullAccountAssetsInfo(address: String): Either[ApiError, JsObject] =
    (for {
      acc <- Address.fromString(address)
    } yield {
      Json.obj(
        "address" -> acc.address,
        "balances" -> JsArray((for {
          (assetId, balance) <- blockchain.portfolio(acc).assets
          if balance > 0
          assetInfo <- blockchain.assetDescription(assetId)
          issueTxOpt = blockchain.transactionInfo(assetId).map(_._2)
          sponsorBalance = if (assetInfo.sponsorshipIsEnabled) {
            Some(blockchain.westPortfolio(assetInfo.issuer.toAddress).spendableBalance)
          } else {
            None
          }
        } yield {
          Json.obj(
            "assetId"              -> assetId.base58,
            "balance"              -> balance,
            "reissuable"           -> assetInfo.reissuable,
            "sponsorshipIsEnabled" -> assetInfo.sponsorshipIsEnabled,
            "sponsorBalance"       -> sponsorBalance,
            "quantity"             -> JsNumber(BigDecimal(assetInfo.totalVolume)),
            "issueTransaction"     -> issueTxOpt.map(_.json())
          )
        }).toSeq)
      )
    }).left.map(ApiError.fromCryptoError)

  private def assetDetails(assetId: String, full: Boolean): Either[ApiError, JsObject] =
    (for {
      id    <- ByteStr.decodeBase58(assetId).toOption.toRight("Incorrect asset ID")
      asset <- blockchain.assetDescription(id).toRight(s"Failed to get description of the asset '$assetId'")
      script = asset.script.filter(_ => full)
      complexity <- script.fold[Either[String, Long]](Right(0))(script => ScriptCompiler.estimate(script, script.version))
    } yield {
      JsObject(
        Seq(
          "assetId"              -> JsString(id.base58),
          "issueHeight"          -> JsNumber(asset.height),
          "issueTimestamp"       -> JsNumber(asset.timestamp),
          "issuer"               -> JsString(asset.issuer.toString),
          "name"                 -> JsString(asset.name),
          "description"          -> JsString(asset.description),
          "decimals"             -> JsNumber(asset.decimals.toInt),
          "reissuable"           -> JsBoolean(asset.reissuable),
          "quantity"             -> JsNumber(BigDecimal(asset.totalVolume)),
          "scripted"             -> JsBoolean(asset.script.nonEmpty),
          "sponsorshipIsEnabled" -> JsBoolean(asset.sponsorshipIsEnabled)
        ) ++ (script.toSeq.map { script =>
          "scriptDetails" -> Json.obj(
            "scriptComplexity" -> JsNumber(BigDecimal(complexity)),
            "script"           -> JsString(script.bytes().base64),
            "scriptText"       -> JsString(script.text)
          )
        })
      )
    }).left.map(m => CustomValidationError(m))
}

object AssetsApiRoute {
  val MaxAddressesPerRequest = 1000
  val MAX_DISTRIBUTION_TASKS = 5

  type DistributionParams = (AssetId, Int, Int, Option[Address])

  def validateDistributionParams(blockchain: Blockchain,
                                 assetParam: String,
                                 heightParam: Int,
                                 limitParam: Int,
                                 maxLimit: Int,
                                 afterParam: Option[String]): Either[ValidationError, DistributionParams] = {
    for {
      limit   <- validateLimit(limitParam, maxLimit)
      height  <- validateHeight(blockchain, heightParam)
      assetId <- validateAssetId(assetParam)
      after   <- afterParam.traverse[Either[ValidationError, ?], Address](Address.fromString(_).leftMap(ValidationError.fromCryptoError))
    } yield (assetId, height, limit, after)
  }

  def validateAssetId(assetParam: String): Either[ValidationError, AssetId] = {
    for {
      _ <- Either.cond(assetParam.length <= AssetIdStringLength, (), GenericError("Unexpected assetId length"))
      assetId <- Base58
        .decode(assetParam)
        .fold(
          _ => GenericError("Must be base58-encoded assetId").asLeft[AssetId],
          arr => ByteStr(arr).asRight[ValidationError]
        )
    } yield assetId
  }

  def validateHeight(blockchain: Blockchain, height: Int): Either[ValidationError, Int] = {
    for {
      _ <- Either
        .cond(height > 0, (), GenericError(s"Height should be greater than zero"))
      _ <- Either
        .cond(height < blockchain.height, (), GenericError(s"Using 'assetDistributionAtHeight' on current height can lead to inconsistent result"))
    } yield height

  }

  def validateLimit(limit: Int, maxLimit: Int): Either[ValidationError, Int] = {
    for {
      _ <- Either
        .cond(limit > 0, (), GenericError("Limit should be greater than 0"))
      _ <- Either
        .cond(limit < maxLimit, (), GenericError(s"Limit should be less than $maxLimit"))
    } yield limit
  }
}
