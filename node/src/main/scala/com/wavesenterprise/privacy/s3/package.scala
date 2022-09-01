package com.wavesenterprise.privacy

import com.wavesenterprise.privacy.s3.S3Error

package object aws {
  type S3Result[T] = Either[S3Error, T]
}
