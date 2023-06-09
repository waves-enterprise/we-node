package com.wavesenterprise.state.diffs

import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.implicits._
import cats.kernel.Monoid
import com.wavesenterprise.metrics.Instrumented
import com.wavesenterprise.state.{Account, Blockchain, Contract, Diff}
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.BalanceErrors
import com.wavesenterprise.utils.ScorexLogging

object BalanceDiffValidation extends ScorexLogging with Instrumented {

  def apply(blockchain: Blockchain, diff: Diff): Either[ValidationError, Diff] = {
    val portfoliosValidationResults: Validated[BalanceErrors, Unit] = diff.portfolios.toList.map {
      case (assetHolder, portfolioDiff) =>
        // WEST balance is all owned WEST tokens, including those that are leased to other accounts
        val oldWestBalance: Long = blockchain.assetHolderBalance(assetHolder)
        val westDelta            = portfolioDiff.balance
        val newWestBalance       = oldWestBalance + westDelta

        // checking WEST balance as a whole
        val westBalanceValidation: ValidatedNel[String, Unit] =
          Validated.condNel(newWestBalance >= 0, (), s"negative WEST balance: ${assetHolder.description}, old: $oldWestBalance, new: $newWestBalance")

        // checking leasing and WEST balance changes regarding to leases
        val leasingValidation: ValidatedNel[String, Unit] = assetHolder match {
          case Contract(contractId) if westDelta < 0 =>
            val leaseBalance    = blockchain.contractLeaseBalance(contractId)
            val newLeaseBalance = leaseBalance |+| portfolioDiff.lease

            val validations = List(
              Validated.condNel(newLeaseBalance.out >= 0, (), s"cannot lease-out negative amount"),
              Validated.condNel(newLeaseBalance.in == 0, (), s"contract lease-in not supported"),
              Validated.condNel(newWestBalance - newLeaseBalance.out >= 0, (), s"cannot spend leased balance")
            )

            validations.combineAll
              .leftMap { errors =>
                val errorConditions =
                  s"old(west balance, lease balance): ${(oldWestBalance, leaseBalance)}, new: ${(newWestBalance, newLeaseBalance)}"
                NonEmptyList.one(errors.toList.mkString("[", ", ", "]") + ": " + errorConditions)
              }

          case Account(sender) if westDelta < 0 =>
            val leaseBalance    = blockchain.addressLeaseBalance(sender)
            val newLeaseBalance = Monoid.combine(leaseBalance, portfolioDiff.lease)

            val validations = List(
              Validated.condNel(newLeaseBalance.out >= 0, (), s"cannot lease-out negative amount"),
              Validated.condNel(newLeaseBalance.in >= 0, (), s"cannot have negative lease-in balance"),
              Validated.condNel(newWestBalance - newLeaseBalance.out >= 0, (), s"cannot spend leased balance")
            )

            validations.combineAll
              .leftMap { errors =>
                val errorConditions =
                  s"old(west balance, lease balance): ${(oldWestBalance, leaseBalance)}, new: ${(newWestBalance, newLeaseBalance)}"
                NonEmptyList.one(errors.toList.mkString("[", ", ", "]") + ": " + errorConditions)
              }

          case _ =>
            Validated.Valid(())
        }

        // check asset balance
        val assetsValidation: ValidatedNel[String, Unit] = portfolioDiff.assets
          .foldLeft(Validated.validNel[String, Unit](())) {
            case (validated, (assetId, diffAmount)) =>
              val oldAssetBalance   = blockchain.assetHolderBalance(assetHolder, Some(assetId))
              val newAssetBalance   = oldAssetBalance + diffAmount
              val currentValidation = Validated.condNel(newAssetBalance >= 0, (), s"asset: '$assetId' -> balance: '$newAssetBalance'")
              validated.combine(currentValidation)
          }
          .leftMap { errors =>
            NonEmptyList.one(s"negative asset balance: [${errors.toList.mkString(", ")}]")
          }

        // gather all validations
        // it would result into one Map element per asset holder
        List(westBalanceValidation, leasingValidation, assetsValidation).combineAll
          .leftMap { errors =>
            val errorsString = s"${assetHolder.description} balance validation errors: [${errors.toList.mkString(", ")}]"
            assetHolder match {
              case Account(address) =>
                BalanceErrors(accountErrs = Map(address -> errorsString))
              case Contract(contractId) =>
                BalanceErrors(contractErrs = Map(contractId.byteStr -> errorsString))
            }
          }
    }.combineAll

    portfoliosValidationResults.toEither.map(_ => diff)
  }
}
