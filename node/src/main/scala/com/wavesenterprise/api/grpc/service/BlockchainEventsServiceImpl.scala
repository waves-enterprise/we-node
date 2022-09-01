package com.wavesenterprise.api.grpc.service

import akka.NotUsed
import akka.grpc.scaladsl.Metadata
import akka.stream.scaladsl.Source
import cats.implicits.{catsSyntaxFlatMapOps, catsSyntaxOptionId}
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.grpc.auth.GrpcAuth
import com.wavesenterprise.api.grpc.service.ConnectionsLimiter.ConnectionId
import com.wavesenterprise.api.grpc.utils._
import com.wavesenterprise.api.http.ApiError.BlockDoesNotExist
import com.wavesenterprise.database.PrivacyState
import com.wavesenterprise.protobuf.service.messagebroker.{BlockchainEvent => PbBlockchainEvent, _}
import com.wavesenterprise.protobuf.service.util.events.EventsFilter.{FilterType, EventsFilter => EventsFilterField}
import com.wavesenterprise.protobuf.service.util.events.SubscribeOnRequest.StartFrom
import com.wavesenterprise.protobuf.service.util.events._
import com.wavesenterprise.serialization.ProtoAdapter
import com.wavesenterprise.settings.AuthorizationSettings
import com.wavesenterprise.settings.api.{BlockchainEventsServiceSettings, DisabledHistoryEventsBufferSettings, EnabledHistoryEventsBufferSettings}
import com.wavesenterprise.state.BlockchainEvent.pbByteStringToByteStr
import com.wavesenterprise.state.{
  AppendedBlockHistory,
  BlockAppended,
  Blockchain,
  BlockchainEvent,
  EventResult,
  MicroBlockAppended,
  RollbackCompleted
}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.docker.ExecutableTransaction
import com.wavesenterprise.transaction.{BlockchainUpdater, ProtoSerializableTransaction, Transaction}
import com.wavesenterprise.utils.Time
import monix.eval.Task
import monix.execution.schedulers.SchedulerService
import monix.reactive.{Observable, OverflowStrategy}

class BlockchainEventsServiceImpl(override val authSettings: AuthorizationSettings,
                                  override val nodeOwner: Address,
                                  override val time: Time,
                                  settings: BlockchainEventsServiceSettings,
                                  maxBlockSizeInBytes: Int,
                                  override val blockchainUpdater: Blockchain with BlockchainUpdater,
                                  override val state: Blockchain with PrivacyState,
                                  val scheduler: SchedulerService)
    extends BlockchainEventsServicePowerApi
    with GrpcAuth
    with ConnectionsLimiter
    with HistoryEventsSupport[BlockchainEvent] {

  override def subscribeOn(in: SubscribeOnRequest, metadata: Metadata): Source[PbBlockchainEvent, NotUsed] = {
    withConnectionsLimiter(metadata, in.toProtoString) { connectionId =>
      withAuth(metadata) {
        Observable.defer {
          val txPredicate = BlockchainEventsServiceImpl.getTxPredicate(in.eventsFilters)
          startFrom(in.startFrom, txPredicate, connectionId).map(_.toProto)
        }
      }
    }
  }

  def startFrom(from: SubscribeOnRequest.StartFrom, txPredicate: Transaction => Boolean, connectionId: ConnectionId): Observable[BlockchainEvent] = {
    appendedBlockHistoryEvents(from, connectionId)
      .publishSelector { historyEvents =>
        val realtimeEvents = blockchainUpdater.lastBlockchainEvent.dropUntil(historyEvents.completed)
        collectHeadEvent(realtimeEvents).publishSelector { headEvent =>
          val otherEvents = (headEvent ++ blockchainUpdater.lastBlockchainEvent.dropUntil(headEvent.completed)).doOnNext { event =>
            Task.eval(log.debug(s"[$connectionId] $event"))
          }

          combineEvents(historyEvents, otherEvents, txPredicate, connectionId)
        }
      }
      .publishSelector(_.flatMap {
        case r: RollbackCompleted =>
          val blockSignature = BlockSignature(BlockchainEvent.byteStrToPbByteString(r.returnToSignature).some)
          val rollbackObs    = Observable.defer(startFrom(SubscribeOnRequest.StartFrom.BlockSignature(blockSignature), txPredicate, connectionId))
          r +: rollbackObs
        case other => Observable.pure(other)
      })
      .executeOn(scheduler)
  }

  private def appendedBlockHistoryEvents(startFrom: SubscribeOnRequest.StartFrom, connectionId: ConnectionId): Observable[BlockchainEvent] =
    startFrom match {
      case _: StartFrom.CurrentEvent => Observable.empty[AppendedBlockHistory]
      case _: StartFrom.GenesisBlock => observeFromHeight(1, connectionId)
      case StartFrom.BlockSignature(blockSignature) =>
        blockSignature.lastBlockSignature
          .fold(Observable.raiseError[BlockchainEvent](GenericError("Missing required field 'last_block_signature'").asGrpcServiceException)) {
            signature =>
              observeFromSignature(signature, connectionId)
          }
      case _ =>
        Observable.raiseError(GenericError("Missing required field 'start_from'").asGrpcServiceException)
    }

  override def observeFromHeight(startHeight: Int, connectionId: ConnectionId): Observable[AppendedBlockHistory] = {
    Observable.fromTask(Task(log.debug(s"[$connectionId] Sync starting from height '$startHeight'"))) >> {
      val historyObservable = Observable
        .fromIterable(Stream.from(startHeight) zip Stream.from(1))
        .takeWhile {
          case (height, _) => height <= state.height
        }
        .mapEval {
          case (height, i) =>
            Task(state.blockAt(height))
              .flatMap {
                case None =>
                  log.warn(s"Block not found at height '$height'")
                  Task.raiseError(BlockDoesNotExist(Right(height)).asGrpcServiceException)
                case Some(block) =>
                  Task.now(AppendedBlockHistory(block, height)).map { historyEvent =>
                    if (i < 100 || i % 1000 == 0 || (i < 1000 && i % 100 == 0)) {
                      log.debug(s"[$connectionId] $historyEvent")
                    }
                    historyEvent
                  }
              }
        }

      settings.historyEventsBuffer match {
        case DisabledHistoryEventsBufferSettings =>
          historyObservable
        case EnabledHistoryEventsBufferSettings(sizeInBytes) =>
          historyObservable.asyncBoundary(OverflowStrategy.BackPressure(getEventsBufferSize(sizeInBytes)))
      }
    }
  }

  /**
    * If we subscribed for blockchain events during a microblock mining, we need to take ngState, otherwise, wait until any block is appended.
    * It's also acceptable for rollback or error events.
    * @param events is a realtime events from {@link BlockchainUpdater}
    * @return the first {@link MicroBlockAppended} event with ngState or {@link BlockAppended} as {@link AppendedBlockHistory} or any other blockchain event
    */
  private def collectHeadEvent(events: Observable[BlockchainEvent]): Observable[BlockchainEvent] =
    events
      .map {
        case MicroBlockAppended(_)                         => blockchainUpdater.ngState.map(state => MicroBlockAppended(state.transactions))
        case ba: BlockAppended if ba.transactions.nonEmpty => ba.toHistoryEvent.some
        case otherEvent                                    => otherEvent.some
      }
      .collect {
        case Some(event) => event
      }
      .head

  private def combineEvents(historyEvents: Observable[BlockchainEvent],
                            currentEvents: Observable[BlockchainEvent],
                            txPredicate: Transaction => Boolean,
                            connectionId: ConnectionId) =
    (historyEvents ++ currentEvents)
      .map {
        case eventResult: EventResult => eventResult.withTransactions(eventResult.transactions.filter(txPredicate))
        case anotherEvent             => anotherEvent
      }
      .takeWhileInclusive {
        case _: RollbackCompleted => false
        case _                    => true
      }
      .onErrorHandleWith { err =>
        log.error(s"[$connectionId] Error while sending blockchain events", err)
        Observable.raiseError(err)
      }

  override val maxConnections: Int   = settings.maxConnections
  override val eventSizeInBytes: Int = maxBlockSizeInBytes
}

object BlockchainEventsServiceImpl {
  def apply(authSettings: AuthorizationSettings,
            nodeOwner: Address,
            time: Time,
            settings: BlockchainEventsServiceSettings,
            maxBlockSizeInBytes: Int,
            blockchainUpdater: Blockchain with BlockchainUpdater,
            state: Blockchain with PrivacyState,
            scheduler: SchedulerService): BlockchainEventsServiceImpl =
    new BlockchainEventsServiceImpl(authSettings, nodeOwner, time, settings, maxBlockSizeInBytes, blockchainUpdater, state, scheduler)

  private[service] def getTxPredicate(eventsFilters: Seq[EventsFilter]): Transaction => Boolean = {
    val protoTxFilter = (tx: Transaction) =>
      tx match {
        case _: ProtoSerializableTransaction => true
        case _                               => false
    }

    eventsFilters
      .map { eventsFilter =>
        val predicate = (tx: Transaction) =>
          eventsFilter.eventsFilter match {
            case EventsFilterField.ContractIdFilter(ContractIdFilter(contractIds, _)) =>
              tx match {
                case tx: ExecutableTransaction => contractIds.contains(ProtoAdapter.byteArrayToByteString(tx.contractId.arr))
                case _                         => true
              }
            case EventsFilterField.TxTypeFilter(TxTypeFilter(txTypes, _)) => txTypes.contains(tx.builder.typeId)
            case _                                                        => true
        }

        eventsFilter.filterType match {
          case _: FilterType.FilterOut => ((tx: Transaction) => !predicate(tx))
          case _                       => predicate
        }
      }
      .+:(protoTxFilter)
      .reduce[Transaction => Boolean] {
        case (a, b) => ((tx: Transaction) => a(tx) && b(tx))
      }
  }
}
