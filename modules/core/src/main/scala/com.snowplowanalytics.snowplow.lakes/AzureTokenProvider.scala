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

import java.util.Date

import com.azure.core.credential.TokenRequestContext
import com.azure.identity.DefaultAzureCredentialBuilder

import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee
import org.apache.hadoop.conf.Configuration

/**
 * Creates Azure tokens for using with Hadoop file system. This class if referenced by the spark
 * config setting `fs.azure.account.oauth.provider.type`
 */
class AzureTokenProvider extends CustomTokenProviderAdaptee {

  private var expiryTime: Date    = _
  private var accountName: String = _

  override def initialize(configuration: Configuration, accountName: String): Unit =
    this.accountName = accountName

  override def getAccessToken: String = {
    val creds   = new DefaultAzureCredentialBuilder().build()
    val request = new TokenRequestContext().addScopes(s"https://$accountName/.default")
    val token   = creds.getToken(request).block()
    this.expiryTime = new Date(token.getExpiresAt.toInstant.toEpochMilli)
    token.getToken
  }

  override def getExpiryTime: Date = expiryTime
}
