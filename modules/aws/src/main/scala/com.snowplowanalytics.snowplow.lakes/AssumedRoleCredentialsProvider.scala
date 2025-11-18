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

import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.auth.credentials.{AwsCredentials, AwsCredentialsProvider, DefaultCredentialsProvider}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.s3a.{Constants => S3aConstants}

import java.net.URI
import java.util.concurrent.TimeUnit

/**
 * A credentials provider that can use STS to assume a role
 *
 * Similar to hadoop's in-built `AssumedRoleCredentialsProvider` but with support for an external
 * id.
 *
 * To enable STS, the spark configuration must contain four parameters:
 *   - "fs.s3a.assumed.role.session.name": The AWS iam session name. A default is provided in
 *     application.conf.
 *   - "fs.s3a.assumed.role.session.duration": The AWS iam session duration. A default is provided
 *     in application.conf.
 *   - "fs.s3a.assumed.role.arn": ARN of the AWS role to assume.
 *   - "fs.s3a.assumed.role.session.external.id": External ID to provide when assuming the role.
 *
 * If any required parameter is missing, we fall back to using the default AWS credentials chain,
 * e.g. environment variables, instance profile, or whatever else.
 *
 * @param delegate
 *   The configured credentials provider to which we delegate requests for credentials
 */
class AssumedRoleCredentialsProvider(delegate: AwsCredentialsProvider) extends AwsCredentialsProvider {

  /**
   * Standard constructor invoked by Delta for the LogStore (e.g. DynamoDB)
   *
   * @param conf
   *   The hadoop configuration, provided via spark configuration
   */
  def this(conf: Configuration) =
    this(AssumedRoleCredentialsProvider.getDelegate(conf))

  /**
   * Standard constructor invoked by hadoop for the filesystem
   *
   * @param fsUri
   *   Base URI of this filesystem (not used by us)
   * @param conf
   *   The hadoop configuration, provided via spark configuration
   */
  def this(fsUri: URI, conf: Configuration) =
    this(conf)

  override def resolveCredentials(): AwsCredentials =
    delegate.resolveCredentials()

}

object AssumedRoleCredentialsProvider {

  private def getDelegate(conf: Configuration): AwsCredentialsProvider = {
    val stsOpt = for {
      roleArn <- Option(conf.getTrimmed(S3aConstants.ASSUMED_ROLE_ARN))
      roleSessionName <- Option(conf.getTrimmed(S3aConstants.ASSUMED_ROLE_SESSION_NAME))
      durationSeconds <- Option(conf.getTimeDuration(S3aConstants.ASSUMED_ROLE_SESSION_DURATION, 0L, TimeUnit.SECONDS).toInt)
      externalId <- Option(conf.getTrimmed("fs.s3a.assumed.role.session.external.id"))
    } yield StsAssumeRoleCredentialsProvider.builder
      .stsClient {
        StsClient.builder.defaultsMode(DefaultsMode.AUTO).build
      }
      .refreshRequest { (req: AssumeRoleRequest.Builder) =>
        req
          .roleArn(roleArn)
          .roleSessionName(roleSessionName)
          .durationSeconds(durationSeconds)
          .externalId(externalId)
        ()
      }
      .build
    stsOpt.getOrElse(DefaultCredentialsProvider.builder().build())
  }

}
