package com.wavesenterprise.database.migration

case class MigrationException(databaseName: String, errorVersion: Int, cause: Throwable)
    extends Exception(s"Error during $databaseName schema migration. Can't migrate to version '$errorVersion'", cause)
