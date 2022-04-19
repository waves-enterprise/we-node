package com.wavesenterprise.generator

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.acl.Role
import com.wavesenterprise.generator.utils.{Gen => WGen}
import com.wavesenterprise.settings.{BlockchainSettings, GenesisSettings}
import com.wavesenterprise.transaction.ValidationError
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfterAll, FreeSpec, Matchers}
import pureconfig.ConfigSource

class GenesisBlockGeneratorSpec extends FreeSpec with Matchers with TransactionGen with BeforeAndAfterAll {
  private val filteredRoles = WGen.roles.filterNot(role => role == Role.Permissioner || role == Role.Sender)

  def blockchainSettingsGen(accountsCount: Int,
                            version: Byte = 1,
                            timestamp: Long = GenesisSettings.minTimestamp,
                            senderRoleEnabled: Boolean = false,
                            addSenderRole: Boolean = false,
                            addPermissionerRole: Boolean = true,
                            customInitialBalance: Option[Long] = None): Gen[BlockchainSettings] = {
    val senderRoles       = if (addSenderRole) List(Role.Sender) else Nil
    val permissionerRoles = if (addPermissionerRole) List(Role.Permissioner) else Nil
    val addressWithDataGen = for {
      amount     <- Gen.choose(5, 40).map(_ * 1000000000000L)
      rolesCount <- Gen.choose(1, 4)
      roles      <- Gen.listOfN(rolesCount, Gen.oneOf(filteredRoles)).map(_.distinct ++ senderRoles ++ permissionerRoles)
      address    <- accountGen
    } yield (address, amount, roles)

    for {
      addressesWithData <- Gen.listOfN(accountsCount, addressWithDataGen)
    } yield {
      val initialBalance = addressesWithData.map {
        case (_, amount, _) => amount
      }.sum
      val transactions = addressesWithData
        .map {
          case (account, amount, _) => s"""{recipient: "${account.toAddress.stringRepr}", amount: $amount}"""
        }
        .mkString("[", ",\n", "]")
      val networkParticipants = addressesWithData
        .map {
          case (account, _, roles) => s"""{public-key: "${account.publicKeyBase58}", roles: ${roles.map(_.prefixS).mkString("[", ",\n", "]")}}"""
        }
        .mkString("[", ",\n", "]")
      ConfigSource
        .string {
          s"""node.blockchain {
               |  consensus.type = PoS
               |  type = CUSTOM
               |  owner-address = 3N3XLqz3DQSeUbFeTQh6m6W6Eg1FBdTVPt8
               |  custom {
               |    address-scheme-character = T
               |    functionality {
               |      feature-check-blocks-period = 100
               |      blocks-for-feature-activation = 90
               |      pre-activated-features = {
               |        2 = 0
               |        3 = 0
               |        4 = 0
               |        5 = 0
               |        6 = 0
               |        7 = 0
               |      }
               |    }
               |    genesis {
               |      version: $version
               |      sender-role-enabled: $senderRoleEnabled
               |      average-block-delay: 10000ms
               |      initial-base-target: 200000
               |      block-timestamp: $timestamp
               |      initial-balance: ${customInitialBalance.getOrElse(initialBalance)}
               |      genesis-public-key-base-58: "HQLZLbtQsEPyHLSjyptHuhqhiGoPz3nVYz2j891PRym9ZuiGC6hwKHRktC9Ws8fLic7pvPpZ2DzWZ7UvXMyeKM5"
               |      signature: "5WepTySe3ab5k6WprLJUaFdDZLu71ecLirGN7WnfunDeixeZ8TBJcoYtnPtaksEBPQpRByZrNkKSp9xZrouPhc9K"
               |      transactions = $transactions
               |      network-participants = $networkParticipants
               |    }
               |  }
               |  fees {
               |    base {
               |      issue = 100000000
               |      transfer = 100000
               |      reissue = 100000000
               |      burn = 500000
               |      exchange = 500000
               |      lease = 500000
               |      lease-cancel = 500000
               |      create-alias = 0
               |      mass-transfer = 500000
               |      data = 500000
               |      set-script = 15000000
               |      sponsor-fee = 100000000
               |      set-asset-script = 100000000
               |      permit = 0
               |      disable-contract = 15000000
               |      call-contract = 15000000
               |      create-contract = 100000000
               |      update-contract = 0
               |      register-node = 500000
               |      create-policy = 100000000
               |      update-policy = 100000000
               |      policy-data-hash = 15000000
               |    }
               |
               |    additional {
               |      mass-transfer = 100000
               |      data = 100000
               |    }
               |  }
               |}""".stripMargin
        }
        .at("node.blockchain")
        .loadOrThrow[BlockchainSettings]
    }
  }

  "Genesis V1" - {
    "More than 127 txs should fail" in {
      val blockchainSettings = blockchainSettingsGen(128).sample.get
      val genesisInfo        = GenesisBlockGenerator.getGenesisInfo(blockchainSettings)
      val genesisSettings    = blockchainSettings.custom.genesis.toPlainSettingsUnsafe
      val expectedTxsCount = genesisSettings.transactions.size +
        genesisSettings.networkParticipants.size +
        genesisSettings.networkParticipants.flatMap(_.roles).size

      genesisInfo shouldBe Left(ValidationError.GenericError(s"Too much transactions for genesis block: found '$expectedTxsCount', max count '127'"))
    }

    "sender-role-enabled=false should pass" in {
      val blockchainSettings = blockchainSettingsGen(3).sample.get
      val genesisInfo        = GenesisBlockGenerator.getGenesisInfo(blockchainSettings)

      genesisInfo shouldBe 'right
    }

    "sender-role-enabled=true should fail" in {
      the[IllegalArgumentException] thrownBy blockchainSettingsGen(3, senderRoleEnabled = true).sample.get should have message "requirement failed: Sender role is only supported in genesis version 2"
    }

    "Not a single Permissioner role should fail" in {
      val blockchainSettings = blockchainSettingsGen(64, addPermissionerRole = false).sample.get
      val genesisInfo        = GenesisBlockGenerator.getGenesisInfo(blockchainSettings)

      genesisInfo shouldBe Left(ValidationError.GenericError("[At least one network participant must have the 'permissioner' role]"))
    }

    "Wrong initial balance should fail" in {
      val blockchainSettings = blockchainSettingsGen(10, customInitialBalance = Some(1000000L)).sample.get
      val genesisInfo        = GenesisBlockGenerator.getGenesisInfo(blockchainSettings)

      genesisInfo shouldBe Left(ValidationError.GenericError("[The value of 'initial-balance' has to be equal to the sum of transactions amounts]"))
    }
  }

  "Genesis V2" - {
    "More than 127 txs should pass" in {
      val blockchainSettings = blockchainSettingsGen(128, version = 2).sample.get
      val genesisInfo        = GenesisBlockGenerator.getGenesisInfo(blockchainSettings)

      genesisInfo shouldBe 'right
    }

    "sender-role-enabled=true without a Sender role should fail" in {
      val blockchainSettings = blockchainSettingsGen(128, version = 2, senderRoleEnabled = true).sample.get
      val genesisInfo        = GenesisBlockGenerator.getGenesisInfo(blockchainSettings)

      genesisInfo shouldBe Left(
        ValidationError.GenericError("[Role 'sender' is required for every network participant which has any additional role]"))
    }

    "sender-role-enabled=true with at least one Sender role should pass" in {
      val blockchainSettings = blockchainSettingsGen(128, version = 2, senderRoleEnabled = true, addSenderRole = true).sample.get
      val genesisInfo        = GenesisBlockGenerator.getGenesisInfo(blockchainSettings)

      genesisInfo shouldBe 'right
    }
  }
}
