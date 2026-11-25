/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes

import org.apache.hadoop.conf.Configuration
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, AWSSessionCredentials}
import software.amazon.awssdk.auth.credentials.{AwsSessionCredentials => AwsSessionCredentialsV2}

/**
 * A credentials provider that delegates to the AWS SDK v2 provider
 *
 * Delta still uses the v1 SDK for authentication to the DynamoDB log store
 */
class AssumedRoleCredentialsProviderV1(delegate: AssumedRoleCredentialsProvider) extends AWSCredentialsProvider {

  def this(conf: Configuration) =
    this(new AssumedRoleCredentialsProvider(conf))

  override def getCredentials(): AWSCredentials =
    delegate.resolveCredentials() match {
      case v2: AwsSessionCredentialsV2 =>
        new AWSSessionCredentials {
          def getAWSAccessKeyId() = v2.accessKeyId()
          def getAWSSecretKey()   = v2.secretAccessKey()
          def getSessionToken()   = v2.sessionToken()
        }
      case v2 =>
        new AWSCredentials {
          def getAWSAccessKeyId() = v2.accessKeyId()
          def getAWSSecretKey()   = v2.secretAccessKey()
        }
    }

  override def refresh(): Unit = ()

}
