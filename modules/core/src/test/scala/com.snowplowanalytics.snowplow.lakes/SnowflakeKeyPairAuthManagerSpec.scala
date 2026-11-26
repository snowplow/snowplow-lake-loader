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

import io.circe.parser

import org.apache.iceberg.rest.auth.OAuth2Properties
import org.apache.iceberg.rest.responses.{ErrorResponse, OAuthTokenResponse}
import org.apache.iceberg.rest.{HTTPRequest, ImmutableHTTPRequest, RESTClient, RESTRequest, RESTResponse}
import org.specs2.Specification

import java.net.URI
import java.security.interfaces.RSAPrivateCrtKey
import java.security.spec.RSAPublicKeySpec
import java.security.{AlgorithmParameters, KeyFactory, KeyPair, KeyPairGenerator, MessageDigest, PrivateKey, Signature}
import java.time.Instant
import java.util.{Base64, Map => JMap}
import java.util.function.Consumer

import javax.crypto.{Cipher, EncryptedPrivateKeyInfo, SecretKeyFactory}
import javax.crypto.spec.{IvParameterSpec, PBEKeySpec, PBEParameterSpec}

class SnowflakeKeyPairAuthManagerSpec extends Specification {
  import SnowflakeKeyPairAuthManagerSpec._

  def is = s2"""
  The Snowflake key-pair auth manager should
    parse an unencrypted PKCS#8 private key $e1
    mint a JWT whose signature verifies against the public key $e2
    identify the account and user in upper case, with the key's fingerprint $e3
    derive the fingerprint Snowflake computes from the public key $e4
    derive the token endpoint from the catalog uri $e5
    prefer an explicitly configured token endpoint $e6
    reject a configuration that is missing a required option $e7
    accept the private key inline, as the Snowflake Loader takes it $e8
    accept an inline key that arrived on a single line $e9
    reject a configuration without a private key $e10
    parse an encrypted PKCS#8 private key with its passphrase $e11
    report a private key that is not a PEM $e12
    report a private key that is not RSA $e13
    keep the current token while a renewal fails before it expires $e14
    fail once the current token has expired and renewal still fails $e15
    derive the account from every form of snowflakecomputing.com catalog uri $e16
    name the derived account in the JWT's iss and sub $e17
    keep underscores in the derived account $e18
    prefer an explicitly configured account to the derived one $e19
    report an account that cannot be derived from the catalog uri $e20
    derive the account when the configured account is blank $e21
  """

  def e1 = {
    val parsed = SnowflakeKeyPairAuthManager.parsePrivateKey(pkcs8Pem, None, "test")
    SnowflakeKeyPairAuthManager.publicKeyFingerprint(parsed) must_== expectedFingerprint
  }

  def e2 = {
    val jwt            = SnowflakeKeyPairAuthManager.mintJwt(config(), now)
    val Array(h, p, s) = jwt.split('.')
    val signature      = Signature.getInstance("SHA256withRSA")
    signature.initVerify(publicKey)
    signature.update(s"$h.$p".getBytes("UTF-8"))
    signature.verify(Base64.getUrlDecoder.decode(s)) must beTrue
  }

  def e3 = {
    val jwt     = SnowflakeKeyPairAuthManager.mintJwt(config(account = "myorg-myacct", user = "lake_loader_svc"), now)
    val payload = parser.parse(new String(Base64.getUrlDecoder.decode(jwt.split('.')(1)), "UTF-8")).toOption.get
    val cursor  = payload.hcursor

    (cursor.get[String]("sub").toOption must beSome("MYORG-MYACCT.LAKE_LOADER_SVC")) and
      (cursor.get[String]("iss").toOption must beSome(s"MYORG-MYACCT.LAKE_LOADER_SVC.$expectedFingerprint")) and
      (cursor.get[Long]("iat").toOption must beSome(now.getEpochSecond)) and
      (cursor.get[Long]("exp").toOption must beSome(be_>(now.getEpochSecond)))
  }

  def e4 =
    SnowflakeKeyPairAuthManager.publicKeyFingerprint(privateKey.asInstanceOf[RSAPrivateCrtKey]) must_== expectedFingerprint

  def e5 =
    SnowflakeKeyPairAuthManager.Config
      .from(properties() + ("uri" -> "https://acct.snowflakecomputing.com/polaris/api/catalog/"))
      .oauth2ServerUri must_== "https://acct.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens"

  def e6 =
    SnowflakeKeyPairAuthManager.Config
      .from(properties() + (OAuth2Properties.OAUTH2_SERVER_URI -> "https://elsewhere/tokens"))
      .oauth2ServerUri must_== "https://elsewhere/tokens"

  def e7 =
    SnowflakeKeyPairAuthManager.Config.from(properties() - SnowflakeKeyPairAuthManager.User) must
      throwA[SnowflakeKeyPairAuthManager.MissingOptionError]
  def e8 = {
    val config = SnowflakeKeyPairAuthManager.Config.from(inlineProperties(pkcs8Pem))
    SnowflakeKeyPairAuthManager.publicKeyFingerprint(config.privateKey) must_== expectedFingerprint
  }

  def e9 = {
    val oneLine = pkcs8Pem.linesIterator.mkString
    val config  = SnowflakeKeyPairAuthManager.Config.from(inlineProperties(oneLine))
    SnowflakeKeyPairAuthManager.publicKeyFingerprint(config.privateKey) must_== expectedFingerprint
  }

  def e10 =
    SnowflakeKeyPairAuthManager.Config.from(properties() - SnowflakeKeyPairAuthManager.PrivateKey) must
      throwA[SnowflakeKeyPairAuthManager.MissingOptionError]
  def e11 = {
    val config = SnowflakeKeyPairAuthManager.Config.from(
      inlineProperties(encryptedPkcs8Pem) + (SnowflakeKeyPairAuthManager.Passphrase -> passphrase)
    )
    SnowflakeKeyPairAuthManager.publicKeyFingerprint(config.privateKey) must_== expectedFingerprint
  }

  def e12 =
    SnowflakeKeyPairAuthManager.Config.from(inlineProperties("not a key")) must
      throwA[SnowflakeKeyPairAuthManager.UnparseablePrivateKeyError]

  def e13 = {
    val generator = KeyPairGenerator.getInstance("EC")
    generator.initialize(256)
    SnowflakeKeyPairAuthManager.Config.from(inlineProperties(pem(generator.generateKeyPair().getPrivate.getEncoded))) must
      throwA[SnowflakeKeyPairAuthManager.NonRsaPrivateKeyError]
  }

  def e14 = {
    val client = new TokenClient
    var clock = now
    val session = new SnowflakeKeyPairAuthManager.Session("test", client, config(), () => clock)

    val first = authorization(session.authenticate(request))
    client.failing = true
    clock          = clock.plusSeconds(3500) // past the renewal point, before the token's hour is up
    val during = authorization(session.authenticate(request))
    client.failing = false
    val after = authorization(session.authenticate(request))

    List(first, during, after) must_== List("Bearer token-1", "Bearer token-1", "Bearer token-2")
  }

  def e15 = {
    val client = new TokenClient
    var clock = now
    val session = new SnowflakeKeyPairAuthManager.Session("test", client, config(), () => clock)

    val first = authorization(session.authenticate(request))
    client.failing = true
    clock          = clock.plusSeconds(3600)

    (first must_== "Bearer token-1") and
      (session.authenticate(request) must throwA[RuntimeException]("token endpoint unavailable"))
  }

  def e16 = {
    val hosts = List(
      "myorg-myacct.snowflakecomputing.com" -> "MYORG-MYACCT",
      "myorg-myacct.privatelink.snowflakecomputing.com" -> "MYORG-MYACCT",
      "xy12345.snowflakecomputing.com" -> "XY12345",
      "xy12345.eu-west-1.snowflakecomputing.com" -> "XY12345",
      "xy12345.us-east-2.aws.snowflakecomputing.com" -> "XY12345",
      "xy12345.us-east-1.privatelink.snowflakecomputing.com" -> "XY12345",
      "MYORG-MYACCT.SNOWFLAKECOMPUTING.COM" -> "MYORG-MYACCT",
      "myorg-myacct.snowflakecomputing.com." -> "MYORG-MYACCT"
    )
    hosts.map { case (host, _) => host -> derivedConfig(s"https://$host/polaris/api/catalog").account } must_== hosts
  }

  def e17 = {
    val jwt    = SnowflakeKeyPairAuthManager.mintJwt(derivedConfig("https://myorg-myacct.snowflakecomputing.com/polaris/api/catalog"), now)
    val cursor = parser.parse(new String(Base64.getUrlDecoder.decode(jwt.split('.')(1)), "UTF-8")).toOption.get.hcursor

    (cursor.get[String]("sub").toOption must beSome("MYORG-MYACCT.LAKE_LOADER_SVC")) and
      (cursor.get[String]("iss").toOption must beSome(s"MYORG-MYACCT.LAKE_LOADER_SVC.$expectedFingerprint"))
  }

  def e18 =
    (derivedConfig("https://myorg-my_acct.snowflakecomputing.com/polaris/api/catalog").account must_== "MYORG-MY_ACCT") and
      (derivedConfig("https://myorg-my_acct.snowflakecomputing.com:443/polaris/api/catalog").account must_== "MYORG-MY_ACCT")

  def e19 =
    SnowflakeKeyPairAuthManager.Config
      .from(
        properties() + (SnowflakeKeyPairAuthManager.Account -> "otherorg-otheracct") + ("uri" -> "https://myorg-myacct.snowflakecomputing.com/polaris/api/catalog")
      )
      .account must_== "OTHERORG-OTHERACCT"

  def e20 =
    (derivedConfig("https://catalog.example.com/polaris/api/catalog") must throwA[SnowflakeKeyPairAuthManager.UnderivableAccountError]) and
      (derivedConfig("https://snowflakecomputing.com/polaris/api/catalog") must throwA[
        SnowflakeKeyPairAuthManager.UnderivableAccountError
      ]) and
      (derivedConfig("myorg-myacct.snowflakecomputing.com/polaris/api/catalog") must throwA[
        SnowflakeKeyPairAuthManager.UnderivableAccountError
      ])

  def e21 =
    SnowflakeKeyPairAuthManager.Config
      .from(
        properties() + (SnowflakeKeyPairAuthManager.Account -> " ") + ("uri" -> "https://myorg-myacct.snowflakecomputing.com/polaris/api/catalog")
      )
      .account must_== "MYORG-MYACCT"
}

object SnowflakeKeyPairAuthManagerSpec {

  private val now = Instant.parse("2026-09-22T12:00:00Z")

  private def generateKeyPair(): KeyPair = {
    val generator = KeyPairGenerator.getInstance("RSA")
    generator.initialize(2048)
    generator.generateKeyPair()
  }

  private val keyPair = generateKeyPair()

  private val privateKey = keyPair.getPrivate
  private val publicKey  = keyPair.getPublic

  private def pem(der: Array[Byte], label: String = "PRIVATE KEY"): String =
    s"-----BEGIN $label-----\n" +
      Base64.getMimeEncoder(64, Array('\n'.toByte)).encodeToString(der) +
      s"\n-----END $label-----\n"

  private val pkcs8Pem = pem(privateKey.getEncoded)

  private val passphrase = "correct horse battery staple"

  /**
   * What `openssl pkcs8 -topk8 -v2 aes256` writes: PBES2 with PBKDF2-HMAC-SHA256 and AES-256-CBC
   */
  private val encryptedPkcs8Pem = {
    val algorithm = "PBEWithHmacSHA256AndAES_256"
    val cipher    = Cipher.getInstance(algorithm)
    cipher.init(
      Cipher.ENCRYPT_MODE,
      SecretKeyFactory.getInstance(algorithm).generateSecret(new PBEKeySpec(passphrase.toCharArray)),
      new PBEParameterSpec(new Array[Byte](16), 2048, new IvParameterSpec(new Array[Byte](16)))
    )
    val encrypted = cipher.doFinal(privateKey.getEncoded)
    // The JDK cannot name the cipher's own parameters in an EncryptedPrivateKeyInfo; their PBES2 encoding can
    val parameters = AlgorithmParameters.getInstance("PBES2")
    parameters.init(cipher.getParameters.getEncoded)
    pem(new EncryptedPrivateKeyInfo(parameters, encrypted).getEncoded, "ENCRYPTED PRIVATE KEY")
  }

  /**
   * Independently of the implementation: SHA-256 of the DER public key, as Snowflake reports it.
   */
  private def fingerprintOf(key: PrivateKey): String = {
    val crt = key.asInstanceOf[RSAPrivateCrtKey]
    val derived = KeyFactory
      .getInstance("RSA")
      .generatePublic(new RSAPublicKeySpec(crt.getModulus, crt.getPublicExponent))
    s"SHA256:${Base64.getEncoder.encodeToString(MessageDigest.getInstance("SHA-256").digest(derived.getEncoded))}"
  }

  private val expectedFingerprint = fingerprintOf(privateKey)

  private def authorization(request: HTTPRequest): String =
    request.headers.firstEntry("Authorization").map(_.value).orElse(null)

  private val request: HTTPRequest =
    ImmutableHTTPRequest.builder().baseUri(URI.create("https://catalog")).method(HTTPRequest.HTTPMethod.GET).path("v1/config").build()

  /**
   * Answers every token exchange with a token that lives an hour, and records the JWT each one sent
   */
  private class TokenClient extends RESTClient {
    var secrets: List[String] = Nil
    var failing: Boolean      = false

    override def postForm[T <: RESTResponse](
      path: String,
      formData: JMap[String, String],
      responseType: Class[T],
      headers: JMap[String, String],
      errorHandler: Consumer[ErrorResponse]
    ): T = {
      if (failing) throw new RuntimeException("token endpoint unavailable")
      secrets = secrets :+ formData.get("client_secret")
      responseType.cast(
        OAuthTokenResponse.builder().withToken(s"token-${secrets.size}").withTokenType("bearer").setExpirationInSeconds(3600).build()
      )
    }

    override def head(
      path: String,
      headers: JMap[String, String],
      errorHandler: Consumer[ErrorResponse]
    ): Unit =
      throw new UnsupportedOperationException
    override def delete[T <: RESTResponse](
      path: String,
      responseType: Class[T],
      headers: JMap[String, String],
      errorHandler: Consumer[ErrorResponse]
    ): T = throw new UnsupportedOperationException
    override def get[T <: RESTResponse](
      path: String,
      queryParams: JMap[String, String],
      responseType: Class[T],
      headers: JMap[String, String],
      errorHandler: Consumer[ErrorResponse]
    ): T = throw new UnsupportedOperationException
    override def post[T <: RESTResponse](
      path: String,
      body: RESTRequest,
      responseType: Class[T],
      headers: JMap[String, String],
      errorHandler: Consumer[ErrorResponse]
    ): T = throw new UnsupportedOperationException
    override def close(): Unit = ()
  }

  private def properties(): Map[String, String] =
    Map(
      SnowflakeKeyPairAuthManager.Account -> "MYORG-MYACCT",
      SnowflakeKeyPairAuthManager.User -> "LAKE_LOADER_SVC",
      SnowflakeKeyPairAuthManager.PrivateKey -> pkcs8Pem,
      OAuth2Properties.SCOPE -> "session:role:LAKE_LOADER_ROLE",
      OAuth2Properties.OAUTH2_SERVER_URI -> "https://acct.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens"
    )

  /** A configuration without `snowflake.auth.account`, so the account comes from `uri` */
  private def derivedConfig(uri: String) =
    SnowflakeKeyPairAuthManager.Config.from(properties() - SnowflakeKeyPairAuthManager.Account + ("uri" -> uri))

  private def inlineProperties(pem: String): Map[String, String] =
    properties() + (SnowflakeKeyPairAuthManager.PrivateKey -> pem)

  private def config(account: String = "MYORG-MYACCT", user: String = "LAKE_LOADER_SVC") =
    SnowflakeKeyPairAuthManager.Config.from(
      properties() + (SnowflakeKeyPairAuthManager.Account -> account) + (SnowflakeKeyPairAuthManager.User -> user)
    )
}
