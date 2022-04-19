package com.wavesenterprise.state.diffs

import com.wavesenterprise.state.{EmptyPolicyDiff, PolicyDiffValue, PolicyDiff => RenamedPolicyDiff}
import org.scalatest.{Assertion, FreeSpec, Matchers, Succeeded}
import cats.implicits._
import com.wavesenterprise.TransactionGen
import tools.GenHelper._

class PolicyDiffTest extends FreeSpec with Matchers with TransactionGen {

  private val defaultOwners     = Set(addressGen.generateSample(), addressGen.generateSample())
  private val defaultRecipients = Set(addressGen.generateSample(), addressGen.generateSample())

  "combine two recipients to add" in {
    val recipient_1 = addressGen.generateSample()
    val recipient_2 = addressGen.generateSample()

    val diff1 = PolicyDiffValue(defaultOwners, Set(recipient_1), Set.empty, Set.empty)
    val diff2 = PolicyDiffValue(defaultOwners, Set(recipient_2), Set.empty, Set.empty)

    val result = combinePolicyDiffs(diff1, diff2)
    result shouldBe a[PolicyDiffValue]
    val policyDiffValue = result.asInstanceOf[PolicyDiffValue]
    policyDiffValue.ownersToAdd should contain theSameElementsAs defaultOwners
    policyDiffValue.recipientsToAdd should contain theSameElementsAs Set(recipient_1, recipient_2)
    policyDiffValue.ownersToRemove shouldBe empty
    policyDiffValue.recipientsToRemove shouldBe empty
    checkSetsDisjoinInvariant(result)
  }

  "combine two owners to add" in {
    val owner_1 = addressGen.generateSample()
    val owner_2 = addressGen.generateSample()

    val diff1 = PolicyDiffValue(Set(owner_1), defaultRecipients, Set.empty, Set.empty)
    val diff2 = PolicyDiffValue(Set(owner_2), defaultRecipients, Set.empty, Set.empty)

    val result = combinePolicyDiffs(diff1, diff2)
    result shouldBe a[PolicyDiffValue]
    val policyDiffValue = result.asInstanceOf[PolicyDiffValue]
    policyDiffValue.ownersToAdd should contain theSameElementsAs Set(owner_1, owner_2)
    policyDiffValue.recipientsToAdd should contain theSameElementsAs defaultRecipients
    policyDiffValue.ownersToRemove shouldBe empty
    policyDiffValue.recipientsToRemove shouldBe empty
    checkSetsDisjoinInvariant(result)
  }

  "combine add and remove recipient" in {
    val recipient_1 = addressGen.generateSample()

    val diff1 = PolicyDiffValue(defaultOwners, Set(recipient_1), Set.empty, Set.empty)
    val diff2 = PolicyDiffValue(Set.empty, Set.empty, Set.empty, Set(recipient_1))

    val result = combinePolicyDiffs(diff1, diff2)
    result shouldBe a[PolicyDiffValue]
    val policyDiffValue = result.asInstanceOf[PolicyDiffValue]
    policyDiffValue.ownersToAdd should contain theSameElementsAs defaultOwners
    policyDiffValue.recipientsToAdd shouldBe empty
    policyDiffValue.ownersToRemove shouldBe empty
    policyDiffValue.recipientsToRemove shouldBe Set(recipient_1)
    checkSetsDisjoinInvariant(result)
  }

  "add one recipient and remove another one" in {
    val recipient_1 = addressGen.generateSample()
    val recipient_2 = addressGen.generateSample()

    val diff1 = PolicyDiffValue(defaultOwners, Set(recipient_1), Set.empty, Set.empty)
    val diff2 = PolicyDiffValue(Set.empty, Set.empty, Set.empty, Set(recipient_2))

    val result = combinePolicyDiffs(diff1, diff2)
    result shouldBe a[PolicyDiffValue]
    val policyDiffValue = result.asInstanceOf[PolicyDiffValue]
    policyDiffValue.ownersToAdd should contain theSameElementsAs defaultOwners
    policyDiffValue.recipientsToAdd should contain theSameElementsAs Set(recipient_1)
    policyDiffValue.ownersToRemove shouldBe empty
    policyDiffValue.recipientsToRemove should contain theSameElementsAs Set(recipient_2)
    checkSetsDisjoinInvariant(result)
  }

  "add one recipient and remove another one (owners empty in both cases)" in {
    val recipient_1 = addressGen.generateSample()
    val recipient_2 = addressGen.generateSample()

    val diff1 = PolicyDiffValue(Set.empty, Set(recipient_1), Set.empty, Set.empty)
    val diff2 = PolicyDiffValue(Set.empty, Set.empty, Set.empty, Set(recipient_2))

    val result = combinePolicyDiffs(diff1, diff2)
    result shouldBe a[PolicyDiffValue]
    val policyDiffValue = result.asInstanceOf[PolicyDiffValue]
    policyDiffValue.ownersToAdd shouldBe empty
    policyDiffValue.recipientsToAdd should contain theSameElementsAs Set(recipient_1)
    policyDiffValue.ownersToRemove shouldBe empty
    policyDiffValue.recipientsToRemove should contain theSameElementsAs Set(recipient_2)
    checkSetsDisjoinInvariant(result)
  }

  "remove two different recipients" in {
    val recipient_1 = addressGen.generateSample()
    val recipient_2 = addressGen.generateSample()

    val diff1 = PolicyDiffValue(Set.empty, Set.empty, Set.empty, Set(recipient_1))
    val diff2 = PolicyDiffValue(Set.empty, Set.empty, Set.empty, Set(recipient_2))

    val result = combinePolicyDiffs(diff1, diff2)
    result shouldBe a[PolicyDiffValue]
    val policyDiffValue = result.asInstanceOf[PolicyDiffValue]
    policyDiffValue.ownersToAdd shouldBe empty
    policyDiffValue.recipientsToAdd shouldBe empty
    policyDiffValue.ownersToRemove shouldBe empty
    policyDiffValue.recipientsToRemove should contain theSameElementsAs Set(recipient_1, recipient_2)
    checkSetsDisjoinInvariant(result)
  }

  "remove two different owners" in {
    val owner_1 = addressGen.generateSample()
    val owner_2 = addressGen.generateSample()

    val diff1 = PolicyDiffValue(Set.empty, Set.empty, Set(owner_1), Set.empty)
    val diff2 = PolicyDiffValue(Set.empty, Set.empty, Set(owner_2), Set.empty)

    val result = combinePolicyDiffs(diff1, diff2)
    result shouldBe a[PolicyDiffValue]
    val policyDiffValue = result.asInstanceOf[PolicyDiffValue]
    policyDiffValue.ownersToAdd shouldBe empty
    policyDiffValue.recipientsToAdd shouldBe empty
    policyDiffValue.ownersToRemove should contain theSameElementsAs Set(owner_1, owner_2)
    policyDiffValue.recipientsToRemove shouldBe empty
    checkSetsDisjoinInvariant(result)
  }

  "add one owner and remove another one" in {
    val owner_1 = addressGen.generateSample()
    val owner_2 = addressGen.generateSample()

    val diff1 = PolicyDiffValue(Set(owner_1), defaultRecipients, Set.empty, Set.empty)
    val diff2 = PolicyDiffValue(Set.empty, Set.empty, Set(owner_2), Set.empty)

    val result          = combinePolicyDiffs(diff1, diff2)
    val policyDiffValue = result.asInstanceOf[PolicyDiffValue]
    policyDiffValue.ownersToAdd should contain theSameElementsAs Set(owner_1)
    policyDiffValue.recipientsToAdd should contain theSameElementsAs defaultRecipients
    policyDiffValue.ownersToRemove should contain theSameElementsAs Set(owner_2)
    policyDiffValue.recipientsToRemove shouldBe empty
    checkSetsDisjoinInvariant(result)
  }

  "add one owner and remove another one (recipients empty in both cases)" in {
    val owner_1 = addressGen.generateSample()
    val owner_2 = addressGen.generateSample()

    val diff2 = PolicyDiffValue(Set.empty, Set.empty, Set(owner_2), Set.empty)
    val diff1 = PolicyDiffValue(Set(owner_1), Set.empty, Set.empty, Set.empty)

    val result          = combinePolicyDiffs(diff1, diff2)
    val policyDiffValue = result.asInstanceOf[PolicyDiffValue]
    policyDiffValue.ownersToAdd should contain theSameElementsAs Set(owner_1)
    policyDiffValue.recipientsToAdd shouldBe empty
    policyDiffValue.ownersToRemove should contain theSameElementsAs Set(owner_2)
    policyDiffValue.recipientsToRemove shouldBe empty
    checkSetsDisjoinInvariant(result)
  }

  "remove 10 recipients and add 5 from them" in {
    val recipientsToRemove = (0 to 10).map(_ => addressGen.generateSample()).toSet
    val recipientsToAdd    = recipientsToRemove.take(5)

    val diff1 = PolicyDiffValue(Set.empty, recipientsToAdd, Set.empty, Set.empty)
    val diff2 = PolicyDiffValue(Set.empty, Set.empty, defaultOwners, recipientsToRemove)

    val result = combinePolicyDiffs(diff1, diff2)
    result shouldBe a[PolicyDiffValue]
    val policyDiffValue = result.asInstanceOf[PolicyDiffValue]
    policyDiffValue.ownersToAdd shouldBe empty
    policyDiffValue.recipientsToAdd shouldBe empty
    policyDiffValue.ownersToRemove should contain theSameElementsAs defaultOwners
    policyDiffValue.recipientsToRemove should contain theSameElementsAs recipientsToRemove
    checkSetsDisjoinInvariant(result)
  }

  "add 10 recipients and remove 5 from them" in {
    val recipientsToAdd    = (0 to 10).map(_ => addressGen.generateSample()).toSet
    val recipientsToRemove = recipientsToAdd.take(5)

    val diff1 = PolicyDiffValue(Set.empty, Set.empty, Set.empty, recipientsToRemove)
    val diff2 = PolicyDiffValue(defaultOwners, recipientsToAdd, Set.empty, Set.empty)

    val result = combinePolicyDiffs(diff1, diff2)
    result shouldBe a[PolicyDiffValue]
    val policyDiffValue = result.asInstanceOf[PolicyDiffValue]
    policyDiffValue.ownersToAdd should contain theSameElementsAs defaultOwners
    policyDiffValue.recipientsToAdd should contain theSameElementsAs recipientsToAdd
    policyDiffValue.ownersToRemove shouldBe empty
    policyDiffValue.recipientsToRemove shouldBe empty
    checkSetsDisjoinInvariant(result)
  }

  "remove 10 owners and add 5 from them" in {
    val ownersToRemove = (0 to 10).map(_ => addressGen.generateSample()).toSet
    val ownersToAdd    = ownersToRemove.take(5)

    val diff1 = PolicyDiffValue(ownersToAdd, Set.empty, Set.empty, Set.empty)
    val diff2 = PolicyDiffValue(Set.empty, Set.empty, ownersToRemove, Set.empty)

    val result = combinePolicyDiffs(diff1, diff2)
    result shouldBe a[PolicyDiffValue]
    val policyDiffValue = result.asInstanceOf[PolicyDiffValue]
    policyDiffValue.ownersToAdd shouldBe empty
    policyDiffValue.recipientsToAdd shouldBe empty
    policyDiffValue.ownersToRemove should contain theSameElementsAs ownersToRemove
    policyDiffValue.recipientsToRemove shouldBe empty
    checkSetsDisjoinInvariant(result)
  }

  "add 10 owners and remove 5 from them" in {
    val ownersToAdd    = (0 to 10).map(_ => addressGen.generateSample()).toSet
    val ownersToRemove = ownersToAdd.take(5)

    val diff1 = PolicyDiffValue(Set.empty, Set.empty, ownersToRemove, Set.empty)
    val diff2 = PolicyDiffValue(ownersToAdd, Set.empty, Set.empty, Set.empty)

    val result = combinePolicyDiffs(diff1, diff2)
    result shouldBe a[PolicyDiffValue]
    val policyDiffValue = result.asInstanceOf[PolicyDiffValue]
    policyDiffValue.ownersToAdd should contain theSameElementsAs ownersToAdd
    policyDiffValue.recipientsToAdd shouldBe empty
    policyDiffValue.ownersToRemove shouldBe empty
    policyDiffValue.recipientsToRemove shouldBe empty
    checkSetsDisjoinInvariant(result)
  }

  "combine with empty diff should be the same as original" in {
    val ownersToAdd        = (0 to 3).map(_ => addressGen.generateSample()).toSet
    val recipientsToAdd    = (0 to 4).map(_ => addressGen.generateSample()).toSet
    val ownersToRemove     = (0 to 5).map(_ => addressGen.generateSample()).toSet
    val recipientsToRemove = (0 to 2).map(_ => addressGen.generateSample()).toSet
    val policyDiff         = PolicyDiffValue(ownersToAdd, recipientsToAdd, ownersToRemove, recipientsToRemove)

    val result = combinePolicyDiffs(policyDiff, EmptyPolicyDiff)
    result shouldBe policyDiff
    checkSetsDisjoinInvariant(result)
  }

  private def combinePolicyDiffs(d1: RenamedPolicyDiff, d2: RenamedPolicyDiff): RenamedPolicyDiff = {
    val key: Int                        = 1
    val m1: Map[Int, RenamedPolicyDiff] = Map(key -> d1)
    val m2: Map[Int, RenamedPolicyDiff] = Map(key -> d2)
    val result                          = m1.combine(m2)
    result(key)
  }

  private def checkSetsDisjoinInvariant(diff: RenamedPolicyDiff): Assertion = {
    diff match {
      case EmptyPolicyDiff =>
        Succeeded
      case PolicyDiffValue(ownersToAdd, recipientsToAdd, ownersToRemove, recipientsToRemove) =>
        ownersToAdd.intersect(ownersToRemove) shouldBe empty
        recipientsToAdd.intersect(recipientsToRemove) shouldBe empty
    }
  }
}
