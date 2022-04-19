package com.wavesenterprise.privacy.db

import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Task
import org.postgresql.util.PSQLException
import slick.dbio.{DBIOAction, NoStream}

trait CommonSlickTrait extends ScorexLogging {

  implicit class DbRunTask[R](db: slick.jdbc.JdbcBackend#Database) {

    def task(a: DBIOAction[R, NoStream, Nothing]): Task[Either[DBError, R]] = {
      Task
        .deferFuture(db.run(a))
        .map(res => Right(res))
        .onErrorRecover {
          case psqlErr: PSQLException             => Left(dbErrorFromPSQL(psqlErr))
          case dbAsyncTaskError: DbAsyncTaskError => Left(DBError.ValidationErrorWrap(dbAsyncTaskError.validationError))
          case generalError =>
            log.error("DB error", generalError)
            Left(DBError.UnmappedError)
        }
    }
  }

  private def dbErrorFromPSQL(psqlException: PSQLException): DBError = {
    val errorMsg = Option(psqlException.getServerErrorMessage).fold(psqlException.getMessage)(_.getMessage)
    val fromMapping = psqlStringToErrorMapping.collectFirst {
      case (msg, error) if errorMsg.startsWith(msg) => error
    }
    fromMapping.getOrElse(DBError.UnmappedError)
  }

  private val psqlStringToErrorMapping: Map[String, DBError] = Map(
    "duplicate key value violates unique constraint" -> DBError.DuplicateKey
  )
}
