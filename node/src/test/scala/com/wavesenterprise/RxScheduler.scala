package com.wavesenterprise

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.block.{Block, MicroBlock, SignerData, TxMicroBlock}
import com.wavesenterprise.crypto.SignatureLength
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.state._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.transfer._
import monix.execution.schedulers.SchedulerService
import monix.execution.{Ack, Scheduler}
import monix.reactive.Observer
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

trait RxScheduler extends BeforeAndAfterAll { _: Suite =>

  implicit val implicitScheduler: SchedulerService = Scheduler.singleThread("rx-scheduler")

  def testSchedulerName: String            = "test-scheduler"
  lazy val testScheduler: SchedulerService = Scheduler.singleThread(testSchedulerName)

  def test[A](f: => Future[A]): A = Await.result(f, 10.seconds)

  def send[A](observer: Observer[A])(message: A): Future[Ack] = {
    observer.onNext(message).map { ack =>
      Thread.sleep(500)
      ack
    }
  }

  def byteStr(id: Int): ByteStr = ByteStr(Array.concat(Array.fill(SignatureLength - 1)(0), Array(id.toByte)))

  val signer: PrivateKeyAccount = TestBlock.defaultSigner

  def block(id: Int): Block = {
    val block          = TestBlock.create(Seq.empty)
    val newBlockHeader = block.blockHeader.copy(signerData = SignerData(signer, byteStr(id)))
    block.copy(blockHeader = newBlockHeader)
  }

  def microBlock(total: Int, prev: Int): MicroBlock = {
    val tx = TransferTransactionV2.selfSigned(signer, None, None, 1, 1, 1, signer.toAddress, Array.emptyByteArray).explicitGet()
    TxMicroBlock.buildAndSign(signer, 1L, Seq(tx), byteStr(prev), byteStr(total)).explicitGet()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    implicitScheduler.shutdown()
    testScheduler.shutdown()
  }
}
