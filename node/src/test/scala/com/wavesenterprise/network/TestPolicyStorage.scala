package com.wavesenterprise.network

import cats.implicits._
import com.wavesenterprise.TestTime
import com.wavesenterprise.api.http.{ApiError, PolicyItem}
import com.wavesenterprise.privacy.{PolicyMetaData, PolicyStorage, PrivacyDataType}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.Time
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

object TestPolicyStorage {

  def build(): PolicyStorage = new PolicyStorage {
    type PolicyId = String
    type DataHash = String

    private val state = new ConcurrentHashMap[(PolicyId, DataHash), (ByteStr, PolicyMetaData)]().asScala

    override val time: Time = new TestTime

    override def policyItemExists(
        policyId: String,
        policyItemHash: String
    ): Task[Either[ApiError, Boolean]] =
      state
        .contains(policyId -> policyItemHash)
        .asRight[ApiError]
        .pure[Task]

    override def policyItemData(
        policyId: PolicyId,
        policyItemHash: DataHash
    ): Task[Either[ApiError, Option[ByteStr]]] =
      state
        .get(policyId -> policyItemHash)
        .map { case (data, _) => ByteStr(data.arr) }
        .asRight[ApiError]
        .pure[Task]

    override def internalSavePolicyDataWithMeta(
        policyData: Either[ByteStr, Observable[Byte]],
        policyMeta: PolicyMetaData
    )(implicit s: Scheduler): Task[Either[ApiError, Unit]] = {
      policyData match {
        case Left(bytes) =>
          state
            .put(policyMeta.policyId -> policyMeta.hash, bytes -> policyMeta)
            .fold(1)(_ => 0)
            .asRight[ApiError]
            .map(_ => {})
            .pure[Task]
        case Right(_) => ???
      }

    }

    override def policyItemMeta(
        policyId: String,
        policyItemHash: String
    ): Task[Either[ApiError, Option[PolicyMetaData]]] =
      state
        .get(policyId -> policyItemHash)
        .map { case (_, meta) => meta }
        .asRight[ApiError]
        .pure[Task]

    override def policyItemsMetas(policyIdsWithHashes: Map[String, Set[String]]): Task[Either[ApiError, Seq[PolicyMetaData]]] = ???
    override def internalSavePolicyItem(sendDataReq: PolicyItem)(implicit s: Scheduler): Task[Either[ApiError, Unit]]         = ???
    override def healthCheck: Task[Either[ApiError, Unit]]                                                                    = Task(Right(()))

    override def close(): Unit = {}

    override def policyItemType(policyId: String, policyItemHash: String): Task[Either[ApiError, Option[PrivacyDataType]]] =
      state
        .get(policyId -> policyId)
        .map(_ => PrivacyDataType.Default)
        .asRight[ApiError]
        .pure[Task]

    override def policyItemDataStream(
        policyId: String,
        policyItemHash: String
    )(implicit s: Scheduler): Task[Either[ApiError, Option[Observable[Array[Byte]]]]] = ???
  }
}
