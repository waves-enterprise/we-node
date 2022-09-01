package com.wavesenterprise.api.grpc.service

import akka.NotUsed
import akka.grpc.scaladsl.Metadata
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{Attributes, Materializer}
import cats.data.EitherT
import cats.implicits._
import com.google.protobuf.ByteString
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.grpc.auth.PrivacyGrpcAuth
import com.wavesenterprise.api.grpc.service.PrivacyServiceImpl.StreamProcessingAccumulator.MetaDataAccumulated
import com.wavesenterprise.api.grpc.service.PrivacyServiceImpl.{StartDataLoadingEvent, buildStreamProcessingSetup, prepareStream}
import com.wavesenterprise.api.grpc.utils.{ApiErrorExt, ValidationErrorExt}
import com.wavesenterprise.api.http.ApiError.{IllegalWatcherActionError, PrivacyLargeObjectFeatureIsNotActivated}
import com.wavesenterprise.api.http.service.PrivacyApiService
import com.wavesenterprise.api.http.{ApiError, PolicyItem, PrivacyDataInfo}
import com.wavesenterprise.protobuf.entity.AddressesResponse
import com.wavesenterprise.protobuf.service.privacy.SendLargeDataRequest.Request
import com.wavesenterprise.protobuf.service.privacy.{SendDataRequest => PbSendDataRequest, _}
import com.wavesenterprise.serialization.ProtoAdapter
import com.wavesenterprise.settings.privacy.PrivacyServiceSettings
import com.wavesenterprise.settings.{AuthorizationSettings, NodeMode}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.{PolicyDataHashTransaction, ValidationError}
import com.wavesenterprise.utils.{ScorexLogging, Time}
import monix.catnap.MVar
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{Consumer, Observable, OverflowStrategy}

import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal

class PrivacyServiceImpl(privacyService: PrivacyApiService,
                         val privacyEnabled: Boolean,
                         val authSettings: AuthorizationSettings,
                         val privacyServiceSettings: PrivacyServiceSettings,
                         val nodeOwner: Address,
                         val time: Time,
                         val nodeMode: NodeMode)(implicit val scheduler: Scheduler, mat: Materializer)
    extends PrivacyPublicServicePowerApi
    with PrivacyGrpcAuth
    with ConnectionIdsHolder {
  implicit def byteArrayToByteString(byteStr: Array[Byte]): ByteString  = ByteString.copyFrom(byteStr)
  implicit def byteStrToPbByteString(byteStr: ByteStr): ByteString      = ByteString.copyFrom(byteStr.arr)
  implicit def pbByteStringToByteStr(pbByteString: ByteString): ByteStr = ByteStr(pbByteString.toByteArray)

  override def getPolicyItemData(in: PolicyItemRequest, metadata: Metadata): Future[PolicyItemDataResponse] =
    runWithPrivacyAuth(metadata) {
      privacyService.policyItemData(in.policyId, in.dataHash)
    } { result =>
      Right(PolicyItemDataResponse(result))
    }

  override def getPolicyItemLargeData(in: PolicyItemRequest, metadata: Metadata): Source[PolicyItemDataResponse, NotUsed] = {
    val chunkedStream = Observable.fromTask {
      withPrivacyAuthTask(metadata) {

        val policyItemEither = for {
          _          <- EitherT.cond[Task](privacyService.isLargeObjectFeatureActivated(), (), PrivacyLargeObjectFeatureIsNotActivated)
          policyItem <- EitherT(Task.deferFuture(privacyService.policyItemLargeData(in.policyId, in.dataHash)))
        } yield policyItem

        policyItemEither.valueOrF(error => Task.raiseError(error.asGrpcServiceException))
      }
    }.flatten

    Source.fromPublisher(chunkedStream.map(PolicyItemDataResponse(_)).toReactivePublisher)
  }

  override def getPolicyItemInfo(in: PolicyItemRequest, metadata: Metadata): Future[PolicyItemInfoResponse] =
    runWithPrivacyAuth(metadata) {
      privacyService.policyItemInfo(in.policyId, in.dataHash)
    } { result =>
      Right(ProtoAdapter.toProto(result))
    }

  /** check policy data in storage */
  override def policyItemDataExists(in: PolicyItemRequest, metadata: Metadata): Future[PolicyItemDataExistsResponse] =
    runWithPrivacyAuth(metadata) {
      privacyService.policyDataExists(in.policyId, in.dataHash)
    } { result =>
      Right(PolicyItemDataExistsResponse(result))
    }

  private def runWithPrivacyAuth[T, R](metadata: Metadata)(f: => Future[Either[ApiError, T]])(
      toResponseMapper: T => Either[ApiError, R]): Future[R] = {
    withPrivacyAuthTask(metadata) {
      EitherT(Task.deferFuture(f))
        .subflatMap(toResponseMapper)
        .valueOrF(err => Task.raiseError(err.asGrpcServiceException))
    }.runToFuture
  }

  override def sendData(request: PbSendDataRequest, metadata: Metadata): Future[SendDataResponse] = {
    runWithPrivacyAuth(metadata) {
      (for {
        broadcast   <- EitherT.fromOption[Future](request.metadata.map(_.broadcastTx), ApiError.CustomValidationError("Empty metadata"))
        _           <- EitherT.cond[Future](nodeMode == NodeMode.Default, (), IllegalWatcherActionError)
        policyItem  <- EitherT(Future(buildPolicyItem(request))).leftMap(ApiError.fromValidationError)
        transaction <- EitherT(privacyService.sendData(policyItem, broadcast))
      } yield transaction).value
    } { transaction =>
      Right(SendDataResponse(transaction.version, Some(transaction.toInnerProto)))
    }
  }

  override def sendLargeData(in: Source[SendLargeDataRequest, NotUsed], metadata: Metadata): Future[SendDataResponse] = {
    withPrivacyAuthTask(metadata) {
      val validation: EitherT[Task, ApiError, Unit] =
        for {
          _ <- EitherT.cond[Task](nodeMode == NodeMode.Default, (), IllegalWatcherActionError)
          _ <- EitherT.cond[Task](privacyService.isLargeObjectFeatureActivated(), (), PrivacyLargeObjectFeatureIsNotActivated: ApiError)
        } yield ()

      for {
        _                <- validation.valueOrF(err => Task.raiseError(err.asGrpcServiceException))
        connectionId     <- Task.fromEither(getConnectionId(metadata).leftMap(_.asGrpcServiceException))
        _                <- Task(log.debug(s"[$connectionId] Starting data loading process"))
        processingSetup  <- buildStreamProcessingSetup(connectionId)
        processingFiber  <- prepareStream(in).consumeWith(processingSetup.consumer).start
        dataLoadingStart <- processingSetup.startAwaiting
        dataStream = dataLoadingStart.dataStream
          .flatMap(Observable.fromIterable(_))
          .asyncBoundary(OverflowStrategy.BackPressure(privacyServiceSettings.requestBufferSize.toBytes.toInt))
        tx <- saveStreamToStorage(dataLoadingStart, dataStream) <& processingSetup.endAwaiting
        _  <- processingFiber.join
      } yield {
        log.debug(s"[$connectionId] Data successfully persisted")
        SendDataResponse(tx.version, Some(tx.toInnerProto))
      }
    }
  }.runToFuture

  private def saveStreamToStorage(dataLoadingStart: StartDataLoadingEvent, dataStream: Observable[Byte]): Task[PolicyDataHashTransaction] =
    EitherT
      .fromEither[Task](privacyService.validateLargePolicyItem(dataLoadingStart.request))
      .leftMap(ApiError.fromValidationError)
      .flatMapF { setup =>
        Task.fromFuture {
          privacyService.sendLargeData(dataLoadingStart.request, setup, dataStream, broadcast = dataLoadingStart.broadcast)
        }
      }
      .valueOrF { apiError =>
        Task.raiseError(apiError.asGrpcServiceException)
      }

  private def buildPolicyItem(request: PbSendDataRequest, version: Byte = 3): Either[ValidationError, PolicyItem] =
    for {
      metadata <- request.metadata.toRight(ValidationError.GenericError("Empty metadata"))
      dataInfo <- metadata.info
        .map(info => PrivacyDataInfo(info.filename, info.size, info.timestamp, info.author, info.comment))
        .toRight(ValidationError.GenericError("Empty data info"))
      maybeAtomicBadge <- metadata.atomicBadge
        .map(atomicBadge => ProtoAdapter.fromProto(atomicBadge).map(Option(_)))
        .getOrElse(Right(None))
      data       = pbByteStringToByteStr(request.data)
      feeAssetId = metadata.feeAssetId
    } yield {
      PolicyItem(
        version = version,
        sender = metadata.senderAddress,
        policyId = metadata.policyId,
        data = Right(data),
        hash = metadata.dataHash,
        info = dataInfo,
        fee = metadata.fee,
        feeAssetId = feeAssetId,
        atomicBadge = maybeAtomicBadge,
        password = metadata.password,
        certificatesBytes = metadata.certificates.map(_.toByteArray).toList
      )
    }

  override def hashes(in: PolicyIdRequest, metadata: Metadata): Future[HashesResponse] =
    withAuthTask(metadata) {
      Task.fromEither {
        privacyService
          .policyHashes(in.policyId)
          .map { hashes =>
            HashesResponse(hashes.toSeq)
          }
          .leftMap(_.asGrpcServiceException)
      }
    }.runToFuture

  override def recipients(in: PolicyIdRequest, metadata: Metadata): Future[AddressesResponse] =
    withAuthTask(metadata) {
      Task.fromEither {
        privacyService
          .policyRecipients(in.policyId)
          .map(AddressesResponse(_))
          .leftMap(_.asGrpcServiceException)
      }
    }.runToFuture

  override def owners(in: PolicyIdRequest, metadata: Metadata): Future[AddressesResponse] =
    withAuthTask(metadata) {
      Task.fromEither {
        privacyService
          .policyOwners(in.policyId)
          .map(AddressesResponse(_))
          .leftMap(_.asGrpcServiceException)
      }
    }.runToFuture

  override def forceSync(in: PolicyIdRequest, metadata: Metadata): Future[ForceSyncResponse] =
    withPrivacyAuthTask(metadata) {
      Task
        .fromFuture(privacyService.forceSync(in.policyId, nodeOwner))
        .flatMap { ei =>
          Task.fromEither {
            ei.map(resp => ForceSyncResponse(resp.forceRestarted)).leftMap(_.asGrpcServiceException)
          }
        }
    }.runToFuture
}

object PrivacyServiceImpl extends ScorexLogging {
  sealed trait StreamProcessingAccumulator

  object StreamProcessingAccumulator {
    case object Empty                                                       extends StreamProcessingAccumulator
    case class MetaDataAccumulated(request: PolicyItem, broadcast: Boolean) extends StreamProcessingAccumulator
    case class DataStreaming(queue: MVar[Task, Array[Byte]])                extends StreamProcessingAccumulator
    case object Completed                                                   extends StreamProcessingAccumulator
  }

  case class StartDataLoadingEvent(request: PolicyItem, broadcast: Boolean, dataStream: Observable[Array[Byte]])

  case class StreamProcessingSetup(startAwaiting: Task[StartDataLoadingEvent],
                                   endAwaiting: Task[Unit],
                                   consumer: Consumer[SendLargeDataRequest, StreamProcessingAccumulator])

  private val Terminator = SendLargeDataRequest(Request.File(File(ByteString.EMPTY)))

  def buildStreamProcessingSetup(connectionId: String): Task[StreamProcessingSetup] = {
    import StreamProcessingAccumulator._

    Task.defer {
      val startPromise = Promise[StartDataLoadingEvent]()
      val startTask    = Task.deferFuture(startPromise.future)

      val endPromise = Promise[Unit]()
      val endTask    = Task.deferFuture(endPromise.future)

      val streamConsumer: Consumer[SendLargeDataRequest, StreamProcessingAccumulator] =
        Consumer.foldLeftTask[StreamProcessingAccumulator, SendLargeDataRequest](Empty) { (acc, next) =>
          Task
            .defer {
              (acc, next) match {
                case (Empty, SendLargeDataRequest(request, _)) =>
                  request match {
                    case Request.Metadata(metadata) =>
                      Task.fromEither {
                        processMetadata(metadata).leftMap(ApiError.fromValidationError(_).asGrpcServiceException)
                      } <* Task(log.trace(s"[$connectionId] Received metadata for policy '${metadata.policyId}' with hash '${metadata.dataHash}'"))
                    case Request.File(_) =>
                      Task.raiseError(ApiError.CustomValidationError(s"Metadata part must be in the head of the stream").asGrpcServiceException)
                    case unexpected =>
                      Task.raiseError(ApiError.CustomValidationError(s"Unexpected stream part: '${unexpected.toString}'").asGrpcServiceException)
                  }
                case (MetaDataAccumulated(sendDataRequest, broadcast), SendLargeDataRequest(request, _)) =>
                  request match {
                    case Request.File(data) =>
                      if (data.content.isEmpty) {
                        Task.raiseError(ApiError.CustomValidationError("Empty data stream").asGrpcServiceException)
                      } else {
                        for {
                          queue <- MVar.empty[Task, Array[Byte]]()
                          _     <- queue.put(data.content.toByteArray)
                          stream = Observable.repeatEvalF(queue.take).takeWhile(_.nonEmpty)
                          _ = log.trace {
                            s"[$connectionId] Start stream data loading for policy '${sendDataRequest.policyId}' data '${sendDataRequest.hash}' with first chunk '${data.content.size()}'"
                          }
                          _ = startPromise.success(StartDataLoadingEvent(sendDataRequest, broadcast, stream))
                        } yield DataStreaming(queue)
                      }

                    case unexpected =>
                      Task.raiseError(ApiError.CustomValidationError(s"Unexpected stream part: '${unexpected.toString}'").asGrpcServiceException)
                  }
                case (DataStreaming(queue), SendLargeDataRequest(request, _)) =>
                  request match {
                    case Request.File(data) =>
                      Task.defer {
                        val isLastChunk = data.content.isEmpty

                        if (isLastChunk) {
                          log.trace(s"[$connectionId] Got the last chunk on raw data streaming")
                        } else {
                          log.trace(s"[$connectionId] Accumulate data with '${data.content.size()}' bytes chunk")
                        }

                        (if (isLastChunk) queue.put(Array.empty) else queue.put(data.content.toByteArray)) *>
                          Task {
                            if (isLastChunk) {
                              endPromise.success(())
                              Completed
                            } else {
                              DataStreaming(queue)
                            }
                          }
                      }
                    case unexpected =>
                      Task.raiseError(ApiError.CustomValidationError(s"Unexpected stream part: '${unexpected.toString}'").asGrpcServiceException)
                  }
                case (Completed, _) =>
                  Task.raiseError(ApiError.CustomValidationError(s"Malformed data stream").asGrpcServiceException)
              }
            }
            .onErrorRecoverWith {
              case NonFatal(ex) =>
                Task {
                  if (!startPromise.isCompleted) startPromise.failure(ex)
                  if (!endPromise.isCompleted) endPromise.failure(ex)
                } *> Task.raiseError(ex)
            }
        }

      Task.pure(StreamProcessingSetup(startTask, endTask, streamConsumer))
    }
  }

  def prepareStream(in: Source[SendLargeDataRequest, NotUsed])(implicit mat: Materializer): Observable[SendLargeDataRequest] = {
    Observable.fromReactivePublisher(in.addAttributes(Attributes.inputBuffer(1, 1)).runWith(Sink.asPublisher(fanout = false))) :+ Terminator
  }

  def processMetadata(metadata: SendDataMetadata, version: Byte = 3): Either[ValidationError, MetaDataAccumulated] = {
    for {
      dataInfo <- metadata.info
        .map(info => PrivacyDataInfo(info.filename, info.size, info.timestamp, info.author, info.comment))
        .toRight(ValidationError.GenericError("Empty data info"))
      maybeAtomicBadge <- metadata.atomicBadge
        .map(atomicBadge => ProtoAdapter.fromProto(atomicBadge).map(Option(_)))
        .getOrElse(Right(None))
    } yield {
      val request = PolicyItem(
        version = version,
        sender = metadata.senderAddress,
        policyId = metadata.policyId,
        data = Left(""),
        hash = metadata.dataHash,
        info = dataInfo,
        fee = metadata.fee,
        feeAssetId = metadata.feeAssetId,
        atomicBadge = maybeAtomicBadge,
        password = metadata.password,
        certificatesBytes = metadata.certificates.map(_.toByteArray).toList
      )

      MetaDataAccumulated(request, metadata.broadcastTx)
    }
  }
}
