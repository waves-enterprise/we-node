package com.wavesenterprise.database.snapshot

import com.wavesenterprise.account.Address
import com.wavesenterprise.database.rocksdb.{RocksDBStorage, RocksDBWriter}
import com.wavesenterprise.database.{Keys, WEKeys}
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.state.ByteStr
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import java.util.concurrent.atomic.AtomicInteger

class AsyncSnapshotVerifier(override val state: RocksDBWriter, override val snapshot: RocksDBWriter)(implicit val scheduler: Scheduler)
    extends SnapshotVerifier {

  import AsyncSnapshotVerifier._

  class TaskHolder(task: Task[Unit]) extends (() => Task[Unit]) {
    override def apply(): Task[Unit] = task
  }

  abstract class DomainVerification[T](domain: String) {

    private[this] val progress = new AtomicInteger(0)

    protected def verifyInitial(): Task[Iterable[T]]

    protected def verifyEach(el: T): Observable[TaskHolder]

    def verify(): Observable[TaskHolder] = {
      Observable
        .fromTask(verifyInitial())
        .doOnNext { initial =>
          Task(log.debug(s"Verifying $domain size '${initial.size}'"))
        }
        .flatMap { iterable =>
          Observable.fromIterable(iterable).flatMap { el =>
            verifyEach(el).doOnComplete(markProgress(iterable.size))
          }
        }
    }

    private def markProgress(size: Int): Task[Unit] = Task {
      val count = progress.incrementAndGet()
      if (count % ReportProgressRate == 0 || count == size) {
        log.debug(s"Handled '$count / $size'")
      }
    }
  }

  class AccountsVerification extends DomainVerification[Address]("accounts") {

    override protected def verifyInitial(): Task[Iterable[Address]] = {
      eitherToTask(verifyAccounts())
    }

    override protected def verifyEach(address: Address): Observable[TaskHolder] = {
      Observable
        .fromTask {
          eitherToTask(verifyAccountState(address)) >> eitherToTask(verifyAccountKeys(address))
        }
        .doOnNext { keys =>
          if (keys.nonEmpty) Task(log.debug(s"Account '$address' data keys count '${keys.size}'"))
          else Task.unit
        }
        .map(_ -> new AtomicInteger(0))
        .flatMap {
          case (keys, keysProgress) =>
            Observable.fromIterable(keys).flatMap { key =>
              Observable {
                new TaskHolder(eitherToTask(verifyAccountData(address, key)) >> markKeysProgress(keysProgress, keys.size))
              }
            }
        }
    }

    private def markKeysProgress(keysProgress: AtomicInteger, size: Int): Task[Unit] = Task {
      val count = keysProgress.incrementAndGet()
      if (count % ReportProgressRate == 0 || count == size) {
        log.debug(s"Handled keys '$count / $size'")
      }
    }
  }

  class AssetsVerification extends DomainVerification[ByteStr]("assets") {

    override protected def verifyInitial(): Task[Iterable[ByteStr]] = {
      eitherToTask(verifyAssets())
    }

    override protected def verifyEach(assetId: ByteStr): Observable[TaskHolder] = {
      Observable(new TaskHolder(eitherToTask(verifyAssetState(assetId))))
    }
  }

  class ContractsVerification extends DomainVerification[ContractInfo]("contracts") {

    override protected def verifyInitial(): Task[Iterable[ContractInfo]] = {
      eitherToTask(verifyContracts())
    }

    override protected def verifyEach(contract: ContractInfo): Observable[TaskHolder] = {
      Observable
        .fromTask {
          eitherToTask(verifyContractKeys(contract.contractId))
        }
        .doOnNext { keys =>
          if (keys.nonEmpty) Task(log.debug(s"Contract '${contract.contractId}' keys count '${keys.size}'"))
          else Task.unit
        }
        .map(_ -> new AtomicInteger(0))
        .flatMap {
          case (keys, keysProgress) =>
            Observable.fromIterable(keys).flatMap { key =>
              Observable {
                new TaskHolder(eitherToTask(verifyContractData(contract.contractId, key)) >> markKeysProgress(keysProgress, keys.size))
              }
            }
        }
    }

    private def markKeysProgress(keysProgress: AtomicInteger, size: Int): Task[Unit] = Task {
      val count = keysProgress.incrementAndGet()
      if (count % ReportProgressRate == 0 || count == size) {
        log.debug(s"Handled contracts keys '$count / $size'")
      }
    }
  }

  class PrivacyVerification extends DomainVerification[ByteStr]("policy") {

    override protected def verifyInitial(): Task[Iterable[ByteStr]] = {
      eitherToTask(verifyPolicies())
    }

    override protected def verifyEach(policy: ByteStr): Observable[TaskHolder] = {
      Observable {
        new TaskHolder(eitherToTask(verifyPolicyState(policy)))
      }
    }
  }

  class NetworkParticipantsVerification extends DomainVerification[Address]("network_participants") {

    override protected def verifyInitial(): Task[Iterable[Address]] = {
      eitherToTask(verifyNetworkParticipants())
    }

    override protected def verifyEach(address: Address): Observable[TaskHolder] = {
      Observable.empty
    }
  }

  class MinersVerification extends DomainVerification[Address]("miners") {
    override protected def verifyInitial(): Task[Iterable[Address]] = eitherToTask(verifyMiners())

    override protected def verifyEach(el: Address): Observable[TaskHolder] = Observable.empty
  }

  class ContractValidatorsVerification extends DomainVerification[Address]("contract_validators") {
    override protected def verifyInitial(): Task[Iterable[Address]] = eitherToTask(verifyContractValidators())

    override protected def verifyEach(el: Address): Observable[TaskHolder] = Observable.empty
  }

  class NonEmptyRoleAddressesVerification extends DomainVerification[Address]("non_empty_role_addresses") {

    override protected def verifyInitial(): Task[Iterable[Address]] = {
      eitherToTask(verifyNonEmptyRoleAddresses())
    }

    override protected def verifyEach(address: Address): Observable[TaskHolder] = {
      Observable.empty
    }
  }

  class CarryFeeVerification extends DomainVerification[Long]("carry_fee") {

    override protected def verifyInitial(): Task[Iterable[Long]] = {
      eitherToTask(verifyCarryFee()) >> Task(Seq.empty)
    }

    override protected def verifyEach(el: Long): Observable[TaskHolder] = {
      Observable.empty
    }
  }

  def verify(): Task[Unit] = {
    val verifications = Seq(
      new AccountsVerification(),
      new AssetsVerification(),
      new ContractsVerification(),
      new PrivacyVerification(),
      new NetworkParticipantsVerification(),
      new MinersVerification(),
      new ContractValidatorsVerification(),
      new NonEmptyRoleAddressesVerification(),
      new CarryFeeVerification()
    )
    verifications
      .foldLeft(Observable.empty[TaskHolder])(_ ++ _.verify())
      .mapParallelUnordered(Parallelism) { _.apply() }
      .completedL
      .executeOn(scheduler)
      .logErr
      .executeAsync
  }

  private def verifyAccountKeys(address: Address): Either[VerificationError, Set[String]] = {
    val stateDataKeys    = readDataKeysSet(address, state.storage)
    val snapshotDataKeys = readDataKeysSet(address, snapshot.storage)
    Either.cond(stateDataKeys == snapshotDataKeys, stateDataKeys, VerificationError(s"Account data is not equal for '$address'"))
  }

  private def verifyAccountData(address: Address, key: String): Either[VerificationError, Unit] = {
    Either.cond(
      snapshot.accountData(address, key) == state.accountData(address, key),
      (),
      VerificationError(s"Account data is not equal for address '$address' and key '$key'")
    )
  }

  private def readDataKeysSet(address: Address, storage: RocksDBStorage): Set[String] = {
    storage
      .get(Keys.addressId(address))
      .map { addressId =>
        Keys.dataKeys(addressId, storage).members
      }
      .getOrElse(Set.empty)
  }

  // TODO: overrides base implementation with RocksDBSet#rawBytes usage because of better performance
  override protected def verifyPolicyState(policy: ByteStr): Either[VerificationError, Unit] = {
    for {
      _ <-
        Either.cond(snapshot.policyExists(policy) == state.policyExists(policy), (), VerificationError(s"'Policy exists' is not equal for '$policy'"))
      _ <- Either.cond(
        WEKeys.policyOwners(snapshot.storage, policy).rawBytes == WEKeys.policyOwners(state.storage, policy).rawBytes,
        (),
        VerificationError(s"Policy owners are not equal for '$policy'")
      )
      _ <- Either.cond(
        WEKeys.policyRecipients(snapshot.storage, policy).rawBytes == WEKeys.policyRecipients(state.storage, policy).rawBytes,
        (),
        VerificationError(s"Policy recipients are not equal for '$policy'")
      )
      _ <- Either.cond(snapshot.policyDataHashes(policy) == state.policyDataHashes(policy),
                       (),
                       VerificationError(s"Policy data hashes are not equal for '$policy'"))
    } yield ()
  }
}

object AsyncSnapshotVerifier {

  private val Parallelism        = (Runtime.getRuntime.availableProcessors() / 2).max(2)
  private val ReportProgressRate = 10000

  private def eitherToTask[T](either: => Either[VerificationError, T]): Task[T] = {
    Task.defer {
      either.fold(err => Task.raiseError(new VerificationException(err.message)), Task.pure)
    }
  }
}

class VerificationException(message: String) extends RuntimeException(message)
