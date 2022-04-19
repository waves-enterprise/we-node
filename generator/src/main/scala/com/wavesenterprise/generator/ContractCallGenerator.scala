package com.wavesenterprise.generator

import cats.Show
import cats.implicits._
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.settings.FeeSettings.FeesEnabled
import com.wavesenterprise.state.{DataEntry, _}
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.transaction.{AssetIdStringLength, Transaction}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Task
import monix.execution.atomic.AtomicLong
import org.asynchttpclient.{AsyncHttpClient, Dsl}
import play.api.libs.json.{JsDefined, JsUndefined, Json}

import java.net.URL
import scala.compat.java8.FutureConverters._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

object ContractCallGenerator {
  final case class Settings(version: Int = 1,
                            transactions: Int,
                            image: String,
                            imageHash: String,
                            contractIds: Seq[String],
                            createParams: List[DataEntry[_]],
                            callParams: List[DataEntry[_]],
                            contractsQuantity: Int,
                            feeAssetId: Option[String]) {
    require(contractIds.nonEmpty || contractsQuantity > 0,
            "`contractsQuantity` setting value should be more that a zero in case `contractIds` setting is empty")
  }

  object Settings {
    implicit val toPrintable: Show[Settings] = { x =>
      import x._
      s"""
         | Version: $version
         | Transactions per iteration: $transactions
         | Image: '$image'
         | Image hash: '$imageHash'
         | Predefined contract ids: $contractIds
         | Create params: $createParams
         | Call params: $callParams
         | Generated contracts quantity: '$contractsQuantity'
         | Fee asset id: $feeAssetId
      """.stripMargin
    }

  }
}

class ContractCallGenerator(settings: ContractCallGenerator.Settings,
                            val accounts: Seq[PrivateKeyAccount],
                            fees: FeesEnabled,
                            httpClient: AsyncHttpClient)
    extends TransactionGenerator
    with BroadcastRequest
    with ScorexLogging {
  private val enoughFee   = fees.forTxType(CallContractTransaction.typeId)
  private val seedCounter = AtomicLong(0)

  private val creator = accounts.head

  private val maybeContractIds =
    if (settings.contractIds.isEmpty) None
    else Some(settings.contractIds.toList.traverse(ByteStr.decodeBase58(_).toEither).explicitGet())

  private def generateCalls: Iterator[Transaction] = {
    val contractVersion = 1
    val callers         = accounts.take(3)
    val random          = new Random
    val callContractIds = maybeContractIds.getOrElse(create.map(_.id()))

    val now = System.currentTimeMillis()
    random.shuffle {
      callContractIds.flatMap { callContractId =>
        Range(0, settings.transactions).map { i =>
          val params = settings.callParams :+ IntegerDataEntry("seed", seedCounter.getAndIncrement())
          CallContractTransactionV2
            .selfSigned(random.shuffle(callers).head, callContractId, params, enoughFee, now + i, contractVersion)
            .explicitGet()
        }
      }
    }.iterator
  }

  private val create: List[CreateContractTransaction] = {
    val now = System.currentTimeMillis()
    val createdTxs = settings.version match {
      case 1 =>
        (1 to settings.contractsQuantity).toList
          .traverse { i =>
            CreateContractTransactionV1
              .selfSigned(creator, settings.image, settings.imageHash, "contract", settings.createParams, enoughFee, now + i)
          }
          .explicitGet()
      case 2 =>
        (for {
          feeAssetId <- parseBase58ToOption(settings.feeAssetId.filter(_.length > 0), "invalid feeAssetId", AssetIdStringLength)
          txs <- (1 to settings.contractsQuantity).toList
            .traverse { i =>
              CreateContractTransactionV2
                .selfSigned(
                  sender = creator,
                  image = settings.image,
                  imageHash = settings.imageHash,
                  contractName = "contract",
                  params = settings.createParams,
                  fee = enoughFee,
                  timestamp = now + i,
                  feeAssetId = feeAssetId
                )
            }
        } yield txs).explicitGet()
      case _ => throw new IllegalArgumentException(s"Unsupported '${CreateContractTransaction.getClass.getSimpleName}' tx version")
    }
    log.info(s"Created contract ids: [${createdTxs.map(_.id()).mkString(",")}]")
    createdTxs
  }

  override def next(): Iterator[Transaction] = generateCalls

  override val initTxs: Seq[Transaction] = if (maybeContractIds.isDefined) super.initTxs else create

  override def afterInitTxsValidation(txs: Seq[Transaction], restUrl: URL): Task[Seq[Boolean]] = Task.sequence {
    txs.map { tx => // TODO add a wiser checks later
      val txId    = tx.id().base58
      val request = Dsl.get(restUrl + s"/transactions/info/$txId").build()
      log.trace(s"Request: '${request.getMethod}/${request.getUrl}'")
      Task
        .deferFuture(httpClient.executeRequest(request).toCompletableFuture.toScala)
        .map { response =>
          Json.parse(response.getResponseBody) \ "id" match {
            case JsDefined(_)   => true
            case _: JsUndefined => false
          }
        }
        .timeout(30 seconds)
        .delayExecution(1 second)
        .restartUntil(identity)
    }
  }
}
