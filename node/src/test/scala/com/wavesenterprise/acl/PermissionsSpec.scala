package com.wavesenterprise.acl

import com.wavesenterprise.NoShrink
import com.wavesenterprise.acl.PermissionsGen._
import com.wavesenterprise.database.{readPermissionSeq, writePermissions}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Random
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class PermissionsSpec extends AnyFreeSpec with Matchers with ScalaCheckPropertyChecks with NoShrink {

  import com.wavesenterprise.acl.OpType._

  "Basic Permissions operations" in {
    forAll(correspondingTimestampsGen, permissionOpWithoutDueGen) { (timestamps, op) =>
      val add    = op.copy(opType = Add, timestamp = timestamps.before)
      val remove = op.copy(opType = Remove, timestamp = timestamps.after)

      val role             = op.role
      val currentTimestamp = timestamps.after + 1

      val p1 = Permissions(Seq(add))
      p1.contains(role, currentTimestamp) shouldBe true
      p1.active(currentTimestamp) shouldBe Set(op.role)

      val p2 = Permissions(Seq(remove))
      p2.contains(role, currentTimestamp) shouldBe false
      p2.active(currentTimestamp) shouldBe Set()

      val p3 = Permissions(Random.shuffle(Seq(add, remove)))
      p3.contains(role, currentTimestamp) shouldBe false
      p3.active(currentTimestamp) shouldBe Set()
    }

    forAll(correspondingTimestampsGen, permissionOpGen, permissionOpGen) { (timestamps, op1, op2) =>
      val add1    = op1.copy(opType = Add, timestamp = timestamps.before)
      val remove1 = op1.copy(opType = Remove, timestamp = timestamps.after)

      val add2             = op2.copy(opType = Add, timestamp = timestamps.before)
      val remove2          = op2.copy(opType = Remove, timestamp = timestamps.after)
      val currentTimestamp = List(add1, remove1, add2, remove2).map(_.timestamp).max + 1

      (Permissions(Seq(add1)) combine Permissions(Seq(add2))).active(currentTimestamp) shouldBe Permissions(Seq(add1, add2)).active(currentTimestamp)
      (Permissions(Seq(remove1, remove2)) combine Permissions(Seq(add1, add2))).active(currentTimestamp) shouldBe Permissions(Seq.empty)
        .active(currentTimestamp)
      (Permissions(Seq(add1, add2)) combine Permissions(Seq(remove1, remove2))).active(currentTimestamp) shouldBe Permissions(Seq.empty)
        .active(currentTimestamp)
    }

    forAll(permissionOpsWithoutDueGen) { allOps =>
      allOps.groupBy(_.role).foreach {
        case (role, ops) =>
          val permissions      = Permissions(ops)
          val currentTimestamp = ops.map(_.timestamp).max
          ops.sortWith(_.timestamp > _.timestamp).headOption match {
            case Some(head) =>
              head.opType match {
                case Add =>
                  permissions.active(currentTimestamp).contains(role) shouldBe true
                  permissions.contains(role, currentTimestamp) shouldBe true
                case Remove =>
                  permissions.active(currentTimestamp).contains(role) shouldBe false
                  permissions.contains(role, currentTimestamp) shouldBe false
              }
            case None => permissions.contains(role, currentTimestamp) shouldBe false
          }
      }
    }

  }

  "PermissionOp" - {
    "serialization size check" in {
      forAll(permissionOpGen) { permOp =>
        val bytes = permOp.bytes
        assert(bytes.length === PermissionOp.serializedSize)
      }
    }
    "serialization round trip" in {
      forAll(permissionOpGen) { permOp =>
        val encoded   = permOp.bytes
        val decodedOp = PermissionOp.fromBytes(encoded).right.get
        assert(decodedOp.role === permOp.role)
        assert(decodedOp.opType === permOp.opType)
        assert(decodedOp.dueTimestampOpt === permOp.dueTimestampOpt)
        assert(decodedOp.timestamp === permOp.timestamp)
      }
    }
    "database serialization round trip" in {
      forAll(permissionOpsGen) { permOpSeq =>
        val encodedBytes   = writePermissions(permOpSeq)
        val decodedPermOps = readPermissionSeq(encodedBytes)
        decodedPermOps should contain theSameElementsAs permOpSeq
      }
    }
  }

  "Permissions active() method" - {
    "doesn't return missing Permissions" in {
      forAll(permissionOpsGen, roleGen) { (opsList, excludedRole) =>
        val opsWithoutExcluded = opsList.filterNot(_.role == excludedRole)
        val active             = Permissions(opsWithoutExcluded).active(Long.MaxValue)
        assert(active.contains(excludedRole) === false)
      }
    }
    "doesn't return expired Premissions" in {
      forAll(permissionOpsGen, permissionOpGen) { (opsList, permOp) =>
        val expiredPermOp     = permOp.copy(opType = OpType.Add, dueTimestampOpt = Some(1L))
        val opsWithoutExpired = opsList.filterNot(_.role == expiredPermOp.role)
        val preparedPermOps   = expiredPermOp :: opsWithoutExpired
        assert(Permissions(preparedPermOps).active(Long.MaxValue).contains(expiredPermOp.role) === false)
      }
    }
    "returns correct Permissions" in {
      forAll(permissionOpsWithoutDueGen) { permOps =>
        val preparedPermOps   = permOps.groupBy(_.role).values.flatten.map(_.copy(opType = OpType.Add, dueTimestampOpt = None)).toSeq
        val activePermissions = Permissions(preparedPermOps).active(Long.MaxValue)
        assert(preparedPermOps.forall(permOp => activePermissions.contains(permOp.role)))
      }
    }
    "doesn't return roles, assigned after specified timestamp" in {
      forAll(permissionOpsWithoutDueGen, timestampGen) { (permOps, roleAssignmentTimestamp) =>
        val preparedPermOps = permOps.groupBy(_.role).values.map(_.head).map(_.copy(opType = OpType.Add, timestamp = roleAssignmentTimestamp)).toSeq
        val permissions     = Permissions(preparedPermOps)
        assert(preparedPermOps.forall(permOp => !permissions.activeAsOps(roleAssignmentTimestamp - 1L).contains(permOp)))
        assert(preparedPermOps.forall(permOp => permissions.activeAsOps(roleAssignmentTimestamp + 1L).contains(permOp)))
      }
    }
  }

  "Permissions contains() method" - {
    "true for active role" in {
      forAll(permissionOpGen) { permOp =>
        assert(Permissions(permOp.copy(opType = OpType.Add, dueTimestampOpt = None) :: Nil).contains(permOp.role, Long.MaxValue))
      }
    }
    "true for active at the given time" in {
      forAll(permissionOpGen, timestampGen) { (permOp, timestamp) =>
        val preparedPermOp = permOp.copy(opType = OpType.Add, timestamp = Long.MinValue, dueTimestampOpt = Some(timestamp))
        assert(Permissions(preparedPermOp :: Nil).contains(preparedPermOp.role, timestamp - 10000))
      }
    }
    "false for inactive at the given time" in {
      forAll(permissionOpGen, timestampGen) { (permOp, timestamp) =>
        val preparedPermOp = permOp.copy(opType = OpType.Add, timestamp = Long.MinValue, dueTimestampOpt = Some(timestamp))
        assert(Permissions(preparedPermOp :: Nil).contains(preparedPermOp.role, timestamp + 1000) === false)
      }
    }
    "false when queried before the role was assigned" in {
      forAll(permissionOpWithoutDueGen, timestampGen) { (permOp, roleAssignmentTimestamp) =>
        val preparedPermOp      = permOp.copy(opType = OpType.Add, timestamp = roleAssignmentTimestamp)
        val preparedPermissions = Permissions(preparedPermOp :: Nil)
        assert(preparedPermissions.contains(preparedPermOp.role, roleAssignmentTimestamp - 1L) === false)
        assert(preparedPermissions.contains(preparedPermOp.role, roleAssignmentTimestamp + 1L) === true)
      }
    }
  }

  "Permissions lastActive() method" - {
    "returns None for empty Permissions" in {
      forAll(roleGen, timestampGen) { (anyRole, ts) =>
        assert(Permissions(Seq.empty).lastActive(anyRole, ts).isEmpty)
      }
    }
    "returns Some for the only active role" in {
      forAll(permissionOpWithoutDueGen) { permOp =>
        val edittedPermOp = permOp.copy(opType = OpType.Add)
        assert(Permissions(Seq(edittedPermOp)).lastActive(permOp.role, Long.MaxValue).isDefined)
      }
    }
    "returns None if queried for Role before it was assigned" in {
      forAll(permissionOpWithoutDueGen) { permOp =>
        val roleAssignmentTimestamp = 1000L
        val edittedPermOp           = permOp.copy(opType = OpType.Add, timestamp = roleAssignmentTimestamp)
        val permissions             = Permissions(Seq(edittedPermOp))
        assert(permissions.lastActive(permOp.role, roleAssignmentTimestamp - 1L).isEmpty)
        assert(permissions.lastActive(permOp.role, roleAssignmentTimestamp + 1L).isDefined)
      }
    }
  }

  "Check ordering" in {
    forAll(permissionsGen) { permissions =>
      val permOpSeq = permissions.toSeq
      val isOrderingCorrect = permOpSeq
        .foldLeft(true -> Long.MaxValue) {
          case ((prevResult, prevTimestamp), permOp) =>
            (prevResult && permOp.timestamp <= prevTimestamp) -> permOp.timestamp
        }
        ._1
      assert(isOrderingCorrect, "Ordering in Permissions is incorrect")
    }
  }

}
