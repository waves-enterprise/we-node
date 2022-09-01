package com.wavesenterprise.api.grpc.service

import akka.actor.ActorSystem
import akka.grpc.GrpcServiceException
import akka.grpc.internal.GrpcMetadataImpl
import akka.stream.scaladsl.Sink
import com.google.protobuf.empty.Empty
import com.wavesenterprise.TestSchedulers.apiComputationsScheduler
import com.wavesenterprise.TestTime
import com.wavesenterprise.api.grpc.utils.ErrorMessageMetadataKey
import com.wavesenterprise.api.http.service.ContractsApiService
import com.wavesenterprise.docker.{ContractExecutionMessage, ContractExecutionMessageGen}
import com.wavesenterprise.protobuf.service.util.ContractExecutionResponse
import com.wavesenterprise.settings.AuthorizationSettings
import com.wavesenterprise.settings.api.ContractStatusServiceSettings
import monix.execution.Scheduler
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{Observable, OverflowStrategy}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ContractStatusServiceImplSpec extends AnyFreeSpec with Matchers with MockFactory with ContractExecutionMessageGen {
  implicit val sys: ActorSystem             = ActorSystem("ContractStatusServiceImplSpec")
  implicit val ec: ExecutionContextExecutor = sys.dispatcher
  implicit val scheduler: Scheduler         = Scheduler(ec)

  private val settings    = ContractStatusServiceSettings(1)
  private val apiKeyHash  = "foo"
  private val authSetting = AuthorizationSettings.ApiKey(apiKeyHash, apiKeyHash)
  private val nodeOwner   = accountGen.sample.get
  private val time        = new TestTime

  private val contractsApiService = mock[ContractsApiService]

  private val emptyMetadata = new GrpcMetadataImpl(new io.grpc.Metadata())

  "#contractsExecutionEvents" - {
    val service = new ContractStatusServiceImpl(settings, contractsApiService, authSetting, nodeOwner.toAddress, time, apiComputationsScheduler)
    "should emmit correct messages" in {
      val subject = ConcurrentSubject.publish[ContractExecutionMessage]
      val obs     = subject.asyncBoundary(OverflowStrategy.Default).takeUntil(subject.completed)
      (contractsApiService.lastMessage _).expects().returns(obs).once()

      val messages     = Gen.listOfN(10, messageGen()).sample.get
      val replySubject = ConcurrentSubject.publish[Unit]
      val reply        = service.contractsExecutionEvents(Empty(), emptyMetadata)
      val publisher    = reply.runWith(Sink.asPublisher[ContractExecutionResponse](fanout = true))
      val collectTask  = Observable.fromReactivePublisher(publisher).takeUntil(replySubject.completed).toListL.executeAsync.runToFuture
      Thread.sleep(2.seconds.toMillis)

      messages.foreach(subject.onNext)
      Thread.sleep(2.seconds.toMillis)
      subject.onComplete()
      Thread.sleep(2.seconds.toMillis)
      replySubject.onComplete()

      val result = Await.result(collectTask, 5.seconds)
      result should contain theSameElementsInOrderAs messages.map(_.toProto)
    }

    "should reject new connections when the limit is exceeded" in {
      val subject = ConcurrentSubject.publish[Unit]
      val obs     = Observable.empty[ContractExecutionMessage]
      (contractsApiService.lastMessage _).expects().returns(obs).once()

      val replyA     = service.contractsExecutionEvents(Empty(), emptyMetadata)
      val replyB     = service.contractsExecutionEvents(Empty(), emptyMetadata)
      val publisherA = replyA.runWith(Sink.asPublisher[ContractExecutionResponse](fanout = true))
      val publisherB = replyB.runWith(Sink.asPublisher[ContractExecutionResponse](fanout = true))
      val taskA      = Observable.fromReactivePublisher(publisherA).takeUntil(subject.completed).toListL.executeAsync.runToFuture
      val taskB      = Observable.fromReactivePublisher(publisherB).takeUntil(subject.completed).toListL.executeAsync.runToFuture

      (the[GrpcServiceException] thrownBy {
        Thread.sleep(2.seconds.toMillis)
        Await.result(taskA, 5.seconds)
        Await.result(taskB, 5.seconds)
      }).metadata.getText(ErrorMessageMetadataKey) shouldBe Some("Max connections count '1' has been exceeded. Please, try again later")

      subject.onComplete()
    }

    "should properly release semaphore" in {
      val subjectSource = ConcurrentSubject.publish[ContractExecutionMessage]
      val messages      = Gen.listOfN(10, messageGen()).sample.get
      val subjectA      = ConcurrentSubject.publish[Unit]
      val subjectB      = ConcurrentSubject.publish[Unit]
      val obs           = subjectSource.asyncBoundary(OverflowStrategy.Default).takeUntil(subjectSource.completed)
      (contractsApiService.lastMessage _).expects().returns(obs).twice()

      val replyA     = service.contractsExecutionEvents(Empty(), emptyMetadata)
      val publisherA = replyA.runWith(Sink.asPublisher[ContractExecutionResponse](fanout = true))
      val taskA      = Observable.fromReactivePublisher(publisherA).takeUntil(subjectA.completed).toListL.executeAsync.runToFuture
      Thread.sleep(1.second.toMillis)
      messages.foreach(subjectSource.onNext)
      Thread.sleep(3.seconds.toMillis)
      subjectA.onComplete()
      Await.result(taskA, 3.seconds).size shouldBe 10
      Thread.sleep(3.seconds.toMillis)

      val replyB     = service.contractsExecutionEvents(Empty(), emptyMetadata)
      val publisherB = replyB.runWith(Sink.asPublisher[ContractExecutionResponse](fanout = true))
      val taskB      = Observable.fromReactivePublisher(publisherB).takeUntil(subjectB.completed).toListL.executeAsync.runToFuture
      Thread.sleep(1.second.toMillis)
      messages.foreach(subjectSource.onNext)
      Thread.sleep(3.seconds.toMillis)
      subjectB.onComplete()
      Await.result(taskB, 3.seconds).size shouldBe 10
    }
  }
}
