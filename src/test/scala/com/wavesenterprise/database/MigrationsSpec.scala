package com.wavesenterprise.database

import com.google.common.primitives.Ints
import com.wavesenterprise.WithDB
import com.wavesenterprise.database.migration.MigrationType.Version
import com.wavesenterprise.database.migration.{Migration, SchemaManager}
import com.wavesenterprise.database.rocksdb.{RW, _}
import org.scalatest.{FlatSpec, Matchers}

class MigrationsSpec extends FlatSpec with Matchers with WithDB {

  import KeyHelpers._

  import scala.util.Random

  override protected def migrateScheme: Boolean = false

  def getSchemaManager: SchemaManager = new SchemaManager(storage)

  "SchemaManager.stateNoneEmpty" should "return false on empty state" in {
    getSchemaManager.stateNoneEmpty shouldBe false
  }

  it should "return true when state none empty" in {
    val schemaManager = getSchemaManager
    schemaManager.updateSchemaVersion(1)
    schemaManager.stateNoneEmpty shouldBe true
  }

  "SchemaManager.getDbVersion" should "be None on empty db" in {
    getSchemaManager.getDbVersion shouldBe None
  }

  it should "return correct values when schema version is updated" in {
    val schemaManager = getSchemaManager
    schemaManager.updateSchemaVersion(1)
    schemaManager.updateSchemaVersion(2)
    schemaManager.getDbVersion shouldBe Some(2)
  }

  def randomIntValues: Stream[Int] = Stream.continually(Random.nextInt(10000))

  def key(id: Int, key: String, prefix: Short): Key[Int] =
    Key(key, h(prefix, id), Option(_).fold(0)(Ints.fromByteArray), Ints.toByteArray)

  def oldKeyFromValue(id: Int): Key[Int] = key(id, "old-key", 123)

  implicit def oldKeyFromBytes(array: Array[Byte]): Key[Int] =
    Key("old-key", array, Option(_).fold(0)(Ints.fromByteArray), Ints.toByteArray)

  def newKey(id: Int): Key[Int] = key(id, "new-key", 321)

  val fineMigrations = List(
    new Migration {
      override def version: Version  = 1
      override def apply: RW => Unit = _ => ()
    },
    new Migration {
      override def version: Version = 2
      override def apply: RW => Unit = { rw: RW =>
        val oldKeys = keysByPrefix(rw.iterator, 123).toList
        oldKeys foreach { oldKey =>
          val value = rw.get(oldKey)
          val key   = newKey(Ints.fromByteArray(oldKey.keyBytes.drop(2)))
          rw.put(key, value)
        }
      }
    }
  )

  "SchemaManager.applyMigrations" should "work on correct list of migrations" in {
    // inserting random key/values to db
    randomIntValues.take(Random.nextInt(1000)) foreach { value =>
      storage.put(oldKeyFromValue(value), value)
    }
    val schemaManager = getSchemaManager
    val oldKeys       = keysByPrefix(storage.newIterator(), 123).toList
    val middleKey     = oldKeys(oldKeys.size / 2)
    val middleValue   = storage.get(middleKey)

    val migrationResult = schemaManager.applyMigrations(fineMigrations)

    val newMiddleKey = newKey(Ints.fromByteArray(middleKey.keyBytes.drop(2)))

    schemaManager.getDbVersion shouldBe Some(2)
    schemaManager.stateNoneEmpty shouldBe true
    storage.get(newMiddleKey) shouldEqual middleValue
    migrationResult.isRight shouldBe true
  }

  it should "fail on incorrect migrations list" in {
    val schemaManager = getSchemaManager
    schemaManager.applyMigrations(errorProneMigrations) shouldBe 'left
  }

  val errorProneMigrations = List(
    new Migration {
      override def version: Version  = 1
      override def apply: RW => Unit = _ => ()
    },
    new Migration {
      override def version: Version = 2
      override def apply: RW => Unit = { rw: RW =>
        rw.put(oldKeyFromValue(322), 1337)
        throw new Exception("Something went wrong")
      }
    }
  )

}
