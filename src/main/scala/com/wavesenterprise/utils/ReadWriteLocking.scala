package com.wavesenterprise.utils

import java.util.concurrent.locks.{Lock, ReadWriteLock}

/**
  * A common wrapper for ReadWrite lock
  */
trait ReadWriteLocking {
  protected val lock: ReadWriteLock
  private def inLock[R](l: Lock, f: => R): R = {
    try {
      l.lock()
      val res = f
      res
    } finally {
      l.unlock()
    }
  }
  protected def writeLock[B](f: => B): B = inLock(lock.writeLock(), f)
  protected def readLock[B](f: => B): B  = inLock(lock.readLock(), f)
}
