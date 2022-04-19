package com.wavesenterprise.privacy

package object db {
  type DBResult[T] = Either[DBError, T]
}
