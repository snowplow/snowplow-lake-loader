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

import io.circe.Json

import org.apache.iceberg.rest.auth.{AuthManager, AuthSession, DefaultAuthSession, OAuth2Properties, OAuth2Util}
import org.apache.iceberg.rest.{HTTPHeaders, HTTPRequest, RESTClient}
import org.slf4j.LoggerFactory

import java.net.URI
import java.security.spec.{PKCS8EncodedKeySpec, RSAPublicKeySpec}
import java.security.interfaces.RSAPrivateCrtKey
import java.security.{KeyFactory, MessageDigest, Signature}
import java.time.Instant
import java.util.{Base64, Collections, Locale, Map => JMap}

import javax.crypto.{Cipher, EncryptedPrivateKeyInfo, SecretKeyFactory}
import javax.crypto.spec.PBEKeySpec

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * Authenticates to Snowflake's Horizon Iceberg REST catalog with a key pair.
 *
 * Selected by setting the Iceberg catalog option `rest.auth.type` to this class's name, e.g.
 *
 * {{{
 * "options": {
 *   "rest.auth.type": "com.snowplowanalytics.snowplow.lakes.SnowflakeKeyPairAuthManager"
 *   "snowflake.auth.user": "LAKE_LOADER_SVC"
 *   "snowflake.auth.private-key": "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----"
 *   "scope": "session:role:LAKE_LOADER_ROLE"
 * }
 * }}}
 *
 * Horizon does not accept a Snowflake key-pair JWT as a bearer token on the catalog endpoint, and
 * it does not implement the `jwt-bearer` grant. The JWT must be exchanged for a catalog token at
 * `<uri>/v1/oauth/tokens`, sent as the `client_secret` of a `client_credentials` request. Both the
 * JWT and the token it buys last an hour.
 *
 * That is why this cannot be Iceberg's own `OAuth2Manager`: it performs the first exchange
 * correctly, but refreshes by re-sending the credential it was given, which for key-pair auth is a
 * JWT that has since expired. This manager mints a fresh JWT for every exchange instead.
 *
 * The JWT names the Snowflake account, which is derived from the catalog `uri` unless
 * `snowflake.auth.account` is set: see [[SnowflakeKeyPairAuthManager.Config.from]].
 */
class SnowflakeKeyPairAuthManager(name: String) extends AuthManager {

  /**
   * Iceberg 1.11's `RESTSessionCatalog.initialize` calls this twice: once through `initSession`,
   * for the `/v1/config` call, and again with the properties merged from that call's response. So
   * startup builds two sessions and performs two token exchanges. That is expected, not a bug.
   */
  override def catalogSession(client: RESTClient, properties: JMap[String, String]): AuthSession =
    new SnowflakeKeyPairAuthManager.Session(name, client, SnowflakeKeyPairAuthManager.Config.from(properties.asScala.toMap))

  override def close(): Unit = ()
}

object SnowflakeKeyPairAuthManager {

  private val logger = LoggerFactory.getLogger(classOf[SnowflakeKeyPairAuthManager])

  final val Account                  = "snowflake.auth.account"
  final val User                     = "snowflake.auth.user"
  final val PrivateKey               = "snowflake.auth.private-key"
  final val Passphrase               = "snowflake.auth.private-key-passphrase"
  private final val Uri              = "uri"
  private final val SnowflakeDomain  = ".snowflakecomputing.com"
  private val JwtTtl: FiniteDuration = 59.minutes // Snowflake rejects a JWT that lives longer than an hour

  /**
   * Renew this long before the catalog token expires, so a request already in flight cannot be
   * rejected for presenting a token that lapsed between the check and the call.
   */
  private val RenewBeforeExpiry: FiniteDuration = 2.minutes

  /**
   * Raised for unusable configuration. Each failure has its own type so it can be told apart from
   * the others.
   *
   * A key that cannot be used (`UnparseablePrivateKeyError`, `NonRsaPrivateKeyError`) and an
   * account that cannot be derived from the catalog `uri` (`UnderivableAccountError`) are reported
   * as destination setup errors, because the key, its passphrase and the catalog URL come from the
   * customer. The other options are written into the config by Snowplow, so a failure there is ours
   * to fix rather than something to alert the customer about.
   */
  sealed abstract class ConfigError(message: String, cause: Throwable) extends RuntimeException(message, cause)

  /** A required catalog option is missing or blank */
  final class MissingOptionError(message: String) extends ConfigError(message, null)

  /** The key is not a PKCS#8 PEM, or its passphrase is wrong or missing */
  final class UnparseablePrivateKeyError(message: String, cause: Throwable) extends ConfigError(message, cause)

  /** The key parses, but it is not an RSA key */
  final class NonRsaPrivateKeyError(message: String) extends ConfigError(message, null)

  /**
   * `snowflake.auth.account` is unset, and the catalog `uri` is not a `snowflakecomputing.com` URL
   */
  final class UnderivableAccountError(message: String) extends ConfigError(message, null)

  final case class Config(
    account: String,
    user: String,
    privateKey: RSAPrivateCrtKey,
    scope: String,
    oauth2ServerUri: String
  )

  object Config {

    /**
     * Snowflake expects the account as either `<ORG>-<ACCOUNT>`, or an account locator without its
     * region (`XY12345`, not `XY12345.US-EAST-1`). The first DNS label of a
     * `snowflakecomputing.com` host is in one of those forms, whether the URL is an org-account, a
     * locator, a regional or a privatelink URL, so that label is the account. So
     * `myorg-myacct.snowflakecomputing.com` and
     * `xy12345.us-east-1.privatelink.snowflakecomputing.com` give `MYORG-MYACCT` and `XY12345`. The
     * label is passed through as written, apart from upper-casing: an underscore is not turned into
     * a hyphen or the other way round. Snowflake's docs do not say which of the two forms a JWT
     * accepts.
     *
     * An explicit `snowflake.auth.account` is used as given, upper-cased, and is not checked: a
     * wrong format shows up as a 401 from the token exchange. It is needed where the derivation
     * gets the account wrong or cannot find one:
     *   - a client-redirect connection URL, `<org>-<connection>.snowflakecomputing.com`, whose
     *     first label names a connection rather than the account;
     *   - a host outside `snowflakecomputing.com`.
     */
    def from(properties: Map[String, String]): Config = {
      val account = optional(properties, Account).getOrElse(deriveAccount(properties))
      val user    = required(properties, User)
      val scope   = required(properties, OAuth2Properties.SCOPE)
      val key     = required(properties, PrivateKey)

      Config(
        account         = account.toUpperCase(Locale.ROOT),
        user            = user.toUpperCase(Locale.ROOT),
        privateKey      = parsePrivateKey(key, properties.get(Passphrase).map(_.toCharArray), s"`$PrivateKey`"),
        scope           = scope,
        oauth2ServerUri = properties.getOrElse(OAuth2Properties.OAUTH2_SERVER_URI, defaultTokenEndpoint(properties))
      )
    }

    private def optional(properties: Map[String, String], key: String): Option[String] =
      properties.get(key).map(_.trim).filter(_.nonEmpty)

    private def required(properties: Map[String, String], key: String): String =
      optional(properties, key) match {
        case Some(value) => value
        case None =>
          throw new MissingOptionError(
            s"Missing Iceberg catalog option `$key`, required by ${classOf[SnowflakeKeyPairAuthManager].getName}"
          )
      }

    private def deriveAccount(properties: Map[String, String]): String = {
      val derived = for {
        host <- hostOf(required(properties, Uri))
        if host.toLowerCase(Locale.ROOT).endsWith(SnowflakeDomain)
        label = host.takeWhile(_ != '.')
        if label.nonEmpty
      } yield label

      derived match {
        case Some(label) => label
        case None =>
          throw new UnderivableAccountError(
            s"Cannot derive the Snowflake account from the catalog `$Uri`, which must be a URL on a `snowflakecomputing.com` host; otherwise set `$Account`"
          )
      }
    }

    /**
     * `java.net.URI` leaves the host null for a name with an underscore, which is not a valid DNS
     * hostname but is a valid Snowflake account URL. The host is then read from the authority.
     *
     * The trailing `.` of a fully qualified name is dropped, so the host can be matched by suffix.
     */
    private def hostOf(uri: String): Option[String] =
      Try(new URI(uri)).toOption.flatMap { parsed =>
        Option(parsed.getHost)
          .orElse(Option(parsed.getRawAuthority).map(_.split('@').last.takeWhile(_ != ':')))
          .map(_.stripSuffix("."))
          .filter(_.nonEmpty)
      }

    private def defaultTokenEndpoint(properties: Map[String, String]): String =
      properties.get(Uri).map(_.stripSuffix("/") + "/v1/oauth/tokens") match {
        case Some(uri) => uri
        case None =>
          throw new MissingOptionError(s"Cannot derive `${OAuth2Properties.OAUTH2_SERVER_URI}`: no catalog `$Uri` is configured")
      }
  }

  /**
   * Parses an unencrypted or encrypted PKCS#8 key. Both are what `openssl pkcs8 -topk8` writes.
   *
   * @param source
   *   Where the key came from, for error messages
   */
  private[lakes] def parsePrivateKey(
    pem: String,
    passphrase: Option[Array[Char]],
    source: String
  ): RSAPrivateCrtKey = {
    val spec =
      try {
        // Remove the armour rather than filtering whole lines: an env var may carry the key on one line
        val der = Base64.getMimeDecoder.decode(pem.replaceAll("-----[A-Z ]+-----", ""))
        passphrase match {
          case Some(chars) =>
            val encrypted = new EncryptedPrivateKeyInfo(der)
            val key       = SecretKeyFactory.getInstance(encrypted.getAlgName).generateSecret(new PBEKeySpec(chars))
            val cipher    = Cipher.getInstance(encrypted.getAlgName)
            cipher.init(Cipher.DECRYPT_MODE, key, encrypted.getAlgParameters)
            encrypted.getKeySpec(cipher)
          case None =>
            new PKCS8EncodedKeySpec(der)
        }
      } catch {
        case e: Exception => throw new UnparseablePrivateKeyError(s"Cannot parse the Snowflake private key from $source", e)
      }

    try
      KeyFactory.getInstance("RSA").generatePrivate(spec) match {
        case crt: RSAPrivateCrtKey => crt
        case _ =>
          throw new UnparseablePrivateKeyError(
            s"The Snowflake private key from $source does not carry its public half, which Snowflake identifies the key by",
            null
          )
      }
    catch {
      case e: ConfigError => throw e
      case e: Exception =>
        otherKeyAlgorithm(spec) match {
          case Some(algorithm) =>
            throw new NonRsaPrivateKeyError(s"Snowflake key-pair auth needs an RSA private key, but the key from $source is $algorithm")
          case None =>
            throw new UnparseablePrivateKeyError(s"Cannot parse the Snowflake private key from $source", e)
        }
    }
  }

  /**
   * The algorithm of a well-formed PKCS#8 key that RSA rejected, to tell the operator what they
   * gave
   */
  private def otherKeyAlgorithm(spec: PKCS8EncodedKeySpec): Option[String] =
    List("EC", "Ed25519", "Ed448", "DSA", "X25519", "X448").find { algorithm =>
      try { KeyFactory.getInstance(algorithm).generatePrivate(spec); true }
      catch { case _: Exception => false }
    }

  /**
   * Mints a Snowflake key-pair JWT.
   *
   * Snowflake identifies the key by the SHA-256 of its public half, and the caller by the account
   * and user that key is registered against, both upper case.
   */
  private[lakes] def mintJwt(config: Config, issuedAt: Instant): String = {
    val privateKey    = config.privateKey
    val qualifiedUser = s"${config.account}.${config.user}"
    val header        = Json.obj("alg" -> Json.fromString("RS256"), "typ" -> Json.fromString("JWT"))
    val payload = Json.obj(
      "iss" -> Json.fromString(s"$qualifiedUser.${publicKeyFingerprint(privateKey)}"),
      "sub" -> Json.fromString(qualifiedUser),
      "iat" -> Json.fromLong(issuedAt.getEpochSecond),
      "exp" -> Json.fromLong(issuedAt.getEpochSecond + JwtTtl.toSeconds)
    )

    val signingInput = s"${base64Url(header.noSpaces)}.${base64Url(payload.noSpaces)}"
    val signature    = Signature.getInstance("SHA256withRSA")
    signature.initSign(privateKey)
    signature.update(signingInput.getBytes("UTF-8"))
    s"$signingInput.${Base64.getUrlEncoder.withoutPadding.encodeToString(signature.sign())}"
  }

  private[lakes] def publicKeyFingerprint(privateKey: RSAPrivateCrtKey): String = {
    val publicKey = KeyFactory
      .getInstance("RSA")
      .generatePublic(new RSAPublicKeySpec(privateKey.getModulus, privateKey.getPublicExponent))
    val digest = MessageDigest.getInstance("SHA-256").digest(publicKey.getEncoded)
    s"SHA256:${Base64.getEncoder.encodeToString(digest)}"
  }

  private def base64Url(value: String): String =
    Base64.getUrlEncoder.withoutPadding.encodeToString(value.getBytes("UTF-8"))

  /**
   * Holds the catalog token, and mints a new JWT to buy another one before it expires.
   *
   * Spark calls the catalog from several threads, so the swap is guarded; the exchange is a single
   * HTTP round trip once an hour, which is not worth a finer-grained scheme.
   */
  private[lakes] class Session(
    catalogName: String,
    client: RESTClient,
    config: Config,
    clock: () => Instant = () => Instant.now()
  ) extends AuthSession {

    /**
     * The token exchange is itself an HTTP call, and it happens while the catalog's own client is
     * inside `authenticate`. Reusing that client re-enters it before its auth session is set, which
     * Iceberg rejects with "Invalid auth session". Deriving a client with an explicit empty session
     * is how Iceberg's own OAuth2Manager avoids this.
     */
    private val tokenClient = client.withAuthSession(AuthSession.EMPTY)

    private var token: String      = _
    private var renewAt: Instant   = Instant.MIN
    private var expiresAt: Instant = Instant.MIN

    /**
     * Blocks the calling thread for the token exchange when a renewal is due. Iceberg's HTTP client
     * is synchronous, so the request this authenticates blocks the same thread anyway, and the
     * loader runs catalog calls on the blocking pool.
     */
    override def authenticate(request: HTTPRequest): HTTPRequest =
      DefaultAuthSession
        .of(HTTPHeaders.of(HTTPHeaders.HTTPHeader.of("Authorization", s"Bearer ${currentToken()}")))
        .authenticate(request)

    override def close(): Unit = ()

    /**
     * A renewal that fails while the current token is still valid is logged and retried on the next
     * request, so a short outage of the token endpoint does not fail catalog calls.
     */
    private def currentToken(): String = synchronized {
      val now = clock()
      if (now.isAfter(renewAt)) {
        try exchange()
        catch {
          case e: Exception if token != null && now.isBefore(expiresAt) =>
            logger.warn(s"Could not renew the Snowflake catalog token on catalog $catalogName; using the current one until $expiresAt", e)
        }
      }
      token
    }

    private def exchange(): Unit = {
      val jwt = mintJwt(config, clock())
      val response = OAuth2Util.fetchToken(
        tokenClient,
        Collections.emptyMap[String, String],
        jwt,
        config.scope,
        config.oauth2ServerUri,
        Collections.emptyMap[String, String]
      )
      val expiresIn = Option(response.expiresInSeconds()).map(_.longValue).getOrElse(JwtTtl.toSeconds)
      val now       = clock()
      token     = response.token()
      expiresAt = now.plusSeconds(expiresIn)
      renewAt   = now.plusSeconds(Math.max(expiresIn - RenewBeforeExpiry.toSeconds, 1L))
      logger.info(
        s"Obtained a Snowflake catalog token for ${config.account}.${config.user} on catalog $catalogName, valid for $expiresIn seconds"
      )
    }
  }
}
