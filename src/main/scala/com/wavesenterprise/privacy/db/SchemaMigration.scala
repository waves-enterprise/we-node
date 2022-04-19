package com.wavesenterprise.privacy.db

import java.net.{ConnectException, UnknownHostException}

import com.wavesenterprise.utils.ScorexLogging
import org.flywaydb.core.Flyway
import org.postgresql.util.PSQLException
import slick.jdbc.DataSourceJdbcDataSource
import slick.jdbc.hikaricp.HikariCPJdbcDataSource

import scala.util.control.NonFatal

object SchemaMigration extends ScorexLogging {

  def migrate(slickDatasource: slick.jdbc.JdbcDataSource,
              migrationDir: String,
              schema: String,
              baseline: Boolean = false,
              shutdown: => Unit = {}): Unit = {

    val ds = slickDatasource match {
      case d: DataSourceJdbcDataSource =>
        d.ds
      case d: HikariCPJdbcDataSource =>
        d.ds
      case other =>
        throw new IllegalStateException("Unknown DataSource type: " + other)
    }

    val flyway = Flyway
      .configure()
      .dataSource(ds)
      .locations(migrationDir)
      .schemas(schema)
      .load()

    try {
      flyway.migrate()
      log.trace("Migration finished")
    } catch {
      case NonFatal(ex) =>
        val knownCauseMessage = Iterator.iterate(ex)(_.getCause).takeWhile(_ != null).toSeq.last match {
          case e: PSQLException if "28P01".equalsIgnoreCase(e.getSQLState) =>
            Some("Wrong authentication data. Check node.privacy.storage.username and password config values")
          case _: UnknownHostException => Some("Wrong database url. Check node.privacy.url config value")
          case e: ConnectException     => Some(s"Check your network or node.privacy.url config value. ${e.getLocalizedMessage}")
          case _                       => None
        }
        knownCauseMessage.map(m => log.error(s"Migration failed. $m")).getOrElse(log.error("Migration failed", ex))
        shutdown
    }
  }
}
