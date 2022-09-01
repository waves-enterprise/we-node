package com.wavesenterprise.history

import com.wavesenterprise.db.WithDomain
import com.wavesenterprise.settings.WESettings
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.{Assertion, Suite}

trait DomainScenarioDrivenPropertyCheck extends WithDomain { _: Suite with ScalaCheckDrivenPropertyChecks =>
  def scenario[S](gen: Gen[S], bs: WESettings = DefaultWESettings)(assertion: (Domain, S) => Assertion): Assertion =
    forAll(gen) { s =>
      withDomain(bs) { domain =>
        assertion(domain, s)
      }
    }
}
