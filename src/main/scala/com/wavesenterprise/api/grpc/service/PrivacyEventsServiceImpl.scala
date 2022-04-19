package com.wavesenterprise.api.grpc.service

import akka.NotUsed
import akka.grpc.scaladsl.Metadata
import akka.stream.scaladsl.Source
import cats.implicits.catsSyntaxFlatMapOps
import com.google.protobuf.{ByteString => PbByteString}
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.grpc.auth.PrivacyGrpcAuth
import com.wavesenterprise.api.grpc.service.ConnectionsLimiter.ConnectionId
import com.wavesenterprise.api.grpc.utils.{ApiErrorExt, ValidationErrorExt}
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.api.http.ApiError.BlockDoesNotExist
import com.wavesenterprise.database.rocksdb.RocksDBWriter
import com.wavesenterprise.privacy.{DataAcquiredEvent, DataInvalidatedEvent, PolicyDataHash, PolicyStorage, PrivacyEvent}
import com.wavesenterprise.protobuf.service.privacy.{PrivacyEventsServicePowerApi, PrivacyEvent => PbPrivacyEvent}
import com.wavesenterprise.protobuf.service.util.events.SubscribeOnRequest
import com.wavesenterprise.protobuf.service.util.events.SubscribeOnRequest.StartFrom
import com.wavesenterprise.settings.AuthorizationSettings
import com.wavesenterprise.settings.api.{DisabledHistoryEventsBufferSettings, EnabledHistoryEventsBufferSettings, PrivacyEventsServiceSettings}
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.{AtomicInnerTransaction, AtomicTransaction, BlockchainUpdater, PolicyDataHashTransaction}
import com.wavesenterprise.utils.Time
import monix.eval.Task
import monix.execution.schedulers.SchedulerService
import monix.reactive.{Observable, OverflowStrategy}

class PrivacyEventsServiceImpl(override val authSettings: AuthorizationSettings,
                               override val nodeOwner: Address,
                               override val time: Time,
                               override val privacyEnabled: Boolean,
                               settings: PrivacyEventsServiceSettings,
                               privacyStorage: PolicyStorage,
                               override val state: RocksDBWriter,
                               override val blockchainUpdater: Blockchain with BlockchainUpdater,
                               val scheduler: SchedulerService)
    extends PrivacyEventsServicePowerApi
    with PrivacyGrpcAuth
    with ConnectionsLimiter
    with HistoryEventsSupport[PrivacyEvent] {

  override val maxConnections: Int   = settings.maxConnections
  override val eventSizeInBytes: Int = PolicyDataHash.DataHashLength + com.wavesenterprise.crypto.DigestSize + java.lang.Long.BYTES

  implicit def pbByteStringToByteStr(pbByteString: PbByteString): ByteStr = ByteStr(pbByteString.toByteArray)

  override def subscribeOn(in: SubscribeOnRequest, metadata: Metadata): Source[PbPrivacyEvent, NotUsed] = {
    validateRequest(in) match {
      case Left(err) => Source.failed(err.asGrpcServiceException)
      case Right(_) =>
        withConnectionsLimiter(metadata, in.toProtoString) { connectionId =>
          withPrivacyAuth(metadata) {
            Observable
              .defer {
                (in.startFrom match {
                  case _: StartFrom.CurrentEvent =>
                    currentEvents.doOnNext { event =>
                      Task.eval(log.debug(s"[$connectionId] $event"))
                    }
                  case _: StartFrom.GenesisBlock => Observable.defer(observeFromHeight(1, connectionId)).executeOn(scheduler)
                  case StartFrom.BlockSignature(blockSignature) =>
                    blockSignature.lastBlockSignature
                      .fold {
                        Observable.raiseError[PrivacyEvent](GenericError("Missing required field 'last_block_signature'").asGrpcServiceException)
                      } { signature =>
                        observeFromSignature(signature, connectionId)
                      }
                  case _ =>
                    Observable.raiseError[PrivacyEvent](GenericError("Missing required field 'start_from'").asGrpcServiceException)
                }).map(_.toProto)
              }
              .executeOn(scheduler)
          }
        }
    }
  }

  private def currentEvents: Observable[PrivacyEvent] =
    Observable(
      privacyStorage.lastPolicyEvent,
      blockchainUpdater.policyRollbacks.map { rollback =>
        DataInvalidatedEvent(rollback.policyId.base58, rollback.dataHash.stringRepr, time.correctedTime())
      }
    ).merge

  override def observeFromHeight(startHeight: Int, connectionId: ConnectionId): Observable[PrivacyEvent] = {
    @volatile var currHeight                 = 0
    @volatile var headRealtimeEventTimestamp = Long.MaxValue
    @volatile var historyFinished            = false
    @volatile var historyEventsCounter       = 0

    Observable.fromTask(Task(log.debug(s"[$connectionId] Sync starting from height '$startHeight'"))) >> {
      val tailRealtimeEvents = currentEvents
        .dropWhile(_ => currHeight < state.height - 2 && !historyFinished)
      val headRealtimeEvent = tailRealtimeEvents
        .find {
          case _: DataAcquiredEvent => true
          case _                    => false
        }
        .doOnNext(e => Task { headRealtimeEventTimestamp = e.timestamp }) >> Observable.empty
      val realtimeEvents = Observable(headRealtimeEvent, tailRealtimeEvents).merge.doOnNext { event =>
        Task(log.debug(s"[$connectionId] $event"))
      }
      val historyObservable: Observable[PrivacyEvent] = Observable
        .fromIterable(Stream.from(startHeight))
        .takeWhile { _ =>
          currHeight < state.height
        }
        .flatMap { height =>
          Observable.fromTask(Task(state.blockAt(height))).flatMap {
            case None =>
              log.warn(s"[$connectionId] Block not found at height '$height'")
              Observable.raiseError(BlockDoesNotExist(Right(height)).asGrpcServiceException)
            case Some(block) if block.timestamp > headRealtimeEventTimestamp =>
              Observable.fromTask(Task { currHeight = state.height + 100 }) >> Observable.empty
            case Some(block) =>
              Observable.fromTask(Task { currHeight = height }) >>
                Observable
                  .fromIterable(block.transactionData.flatMap {
                    case atomic: AtomicTransaction =>
                      atomic.transactions.collect {
                        case pdh: AtomicInnerTransaction with PolicyDataHashTransaction =>
                          DataAcquiredEvent(pdh.policyId.base58, pdh.dataHash.stringRepr, block.timestamp)
                      }
                    case pdh: PolicyDataHashTransaction =>
                      val event = DataAcquiredEvent(pdh.policyId.base58, pdh.dataHash.stringRepr, block.timestamp)
                      event :: Nil
                    case _ => Nil
                  })
                  .doOnNext { event =>
                    Task {
                      historyEventsCounter += 1
                      if (historyEventsCounter < 100 || historyEventsCounter % 1000 == 0 || (historyEventsCounter < 1000 && historyEventsCounter % 100 == 0)) {
                        log.debug(s"[$connectionId] $event")
                      }
                    }
                  }
          }
        }
        .doOnComplete(Task { historyFinished = true })

      val bufferedHistoryEvents = settings.historyEventsBuffer match {
        case DisabledHistoryEventsBufferSettings =>
          historyObservable
        case EnabledHistoryEventsBufferSettings(sizeInBytes) =>
          historyObservable.asyncBoundary(OverflowStrategy.BackPressure(getEventsBufferSize(sizeInBytes)))
      }

      Observable(bufferedHistoryEvents, realtimeEvents).merge
    }
  }

  private def validateRequest(in: SubscribeOnRequest): Either[ApiError, Unit] = {
    Either.cond(in.eventsFilters.isEmpty, (), ApiError.CustomValidationError("Events filters are not supported at the moment"))
  }
}
