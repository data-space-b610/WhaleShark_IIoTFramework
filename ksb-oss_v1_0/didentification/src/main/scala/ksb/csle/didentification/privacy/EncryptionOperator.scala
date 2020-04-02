package ksb.csle.didentification.privacy

import java.security.MessageDigest
import javax.crypto.{KeyGenerator, Cipher}
import javax.crypto.spec.{SecretKeySpec, IvParameterSpec}

import org.apache.commons.codec.binary.Base64

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

import ksb.csle.common.proto.DatasourceProto.FieldInfo
import ksb.csle.common.proto.StreamDidentProto._
import ksb.csle.common.proto.StreamControlProto.StreamOperatorInfo

import ksb.csle.didentification.verification.Verification
import ksb.csle.didentification.utilities.RandomGenerator

/**
 * :: ApplicationDeveloperApi ::
 *
 * Operator that implements the encryption module in the Pseudo Anonymization
 * algorithm, and encrypts the values of the data using encryption algorithm.
 * Currently, DES (Data Encryption Standard) and AES (Advanced DES) are
 * implemented (key size: 56 (DES), 128 (AES)). Especially, due to the
 * importance of the key generation, it makes the key based on SHA1 and SHA512.
 *
 * @param o Object that contains message
 *          [[ksb.csle.common.proto.StreamDidentProto.EncryptionInfo]]
 *          EncryptionInfo contains attributes as follows:
 *          - selectedColumnId: Column ID to apply the encryption function
 *          - valColName: Column name to be used as a value column (required)
 *          - key: the encryption key
 *          - method: DES or AES (currently, DES is not supported)
 *          - fieldInfo: the info about column attributes (identifier, sensitive, ..)
 *          - check: the method how to verify the performance of anonymized data
 *
 *  ==EncryptionInfo==
 *  {{{
 *  enum EncryptionKey {
 *  	NORMAL = 0;
 *  	SHA1 = 1;
 *  	SHA512 = 2;
 *  }
 *  enum EncryptionMethod {
 *  	DES = 0;
 *  	AES = 1;
 *  }
 *  message EncryptionInfo {
 *    repeated int32 selectedColumnId = 1;
 *    required EncryptionKey key = 2 [default = NORMAL];
 *    required EncryptionMethod method = 3 [default = DES];
 *    repeated FieldInfo fieldInfo = 4;
 *    optional PrivacyCheckInfo check = 5;
 *  }
 *  }}}
 */
class EncryptionOperator(o: StreamOperatorInfo)
  extends BasePrivacyAnonymizer(o, o.getEncryption.getCheck) {

  val p: EncryptionInfo = o.getEncryption
  var key: String = ""

  /**
   * encrypts the column of src dataframe using defined encryption method.
   *
   * @param src Dataframe to anonymize
   * @param columnName Column to be pseudo-anonymized
   * @param method the encryption method to apply (AES, DES)
   * @return DataFrame Anonymized dataframe
   */
  override def anonymizeColumn(
      src: DataFrame,
      columnName: String): DataFrame =
        anonymizeColumn(src, columnName, p.getMethod)

  def anonymizeColumn(
      src: DataFrame,
      columnName: String,
      method: EncryptionMethod): DataFrame = {
    key = method match {
      case EncryptionMethod.AES => encodeBase64(generateKey("AES", 128, p.getKey))
      case EncryptionMethod.DES => encodeBase64(generateKey("DES", 56, p.getKey))
      case _ => encodeBase64(generateKey("AES", 128, p.getKey))
    }

    def encryptAnonymize: (String => String) =
      value =>
        method match {
          case EncryptionMethod.AES => encodeBase64(
            encrypt(value.getBytes("UTF-8"), key, "AES"))
          case EncryptionMethod.DES => encodeBase64(
            encrypt(value.getBytes("UTF-8"), key, "DES"))
        }

    val encryptUdf = udf(encryptAnonymize)
    val result = src.withColumn(columnName, encryptUdf(src.col(columnName)))
    //deAnonymize(result, columnName).show
    (result)
  }

  /**
   * De-anonymizes the anonymized dataframe using defined encryption method
   *
   * @param src Dataframe to de-anonymize
   * @param columnName Column name to be de-anonymized
   * @param method the decryption method to apply (AES, DES)
   * @return DataFrame The de-anonymized dataframe
   */
  def deAnonymize(
      src: DataFrame,
      columnName: String): DataFrame =
        deAnonymize(src, columnName, p.getMethod)

  def deAnonymize(
      src: DataFrame,
      columnName: String,
      method: EncryptionMethod): DataFrame = {
    def decryptAnonymize: (String => String) =
      value => {
        method match {
          case EncryptionMethod.AES => new String(
            decrypt(decodeBase64(value), key, "AES"), "UTF-8")
          case EncryptionMethod.DES => new String(
            decrypt(decodeBase64(value), key, "DES"), "UTF-8")
        }
      }

    val decryptUdf = udf(decryptAnonymize)
    src.withColumn(columnName, decryptUdf(src.col(columnName)))
  }

  /**
   * Encrypts the given array of bytes
   *
   * @param bytes the array of bytes in the value to anonymize.
   * @param b64secret key to anonymize
   * @param algo the anonymization method to apply (AES, DES)
   * @return Array[Byte] The array of bytes in the anonymized value
   */
  def encrypt(bytes: Array[Byte], b64secret: String, algo: String)
      : Array[Byte] = {
    val encoder = cipher(Cipher.ENCRYPT_MODE, b64secret, algo)
    encoder.doFinal(bytes)
  }

  /**
   * Decrypts the given array of encrypted bytes
   *
   * @param bytes the array of bytes in the value to be de-anonymized.
   * @param b64secret key to anonymize
   * @param algo the anonymization method to apply (AES, DES)
   * @return Array[Byte] The array of bytes in the deanonymized value
   */
  def decrypt(bytes: Array[Byte], b64secret: String, algo: String)
      : Array[Byte] = {
    val decoder = cipher(Cipher.DECRYPT_MODE, b64secret, algo)
    decoder.doFinal(bytes)
  }

  private def encodeBase64(bytes: Array[Byte]) = Base64.encodeBase64String(bytes)
  private def decodeBase64(string: String) = Base64.decodeBase64(string)

  private def generateKey(algorithm: String, size: Int, keyType: EncryptionKey)
      : Array[Byte] = {
    var keyBytes: Array[Byte] =
      RandomGenerator.randomMixed(100).getBytes("UTF-8")
    keyType match {
      case EncryptionKey.NORMAL => {
        val generator = KeyGenerator.getInstance(algorithm)
        generator.init(size)
        generator.generateKey().getEncoded
      }
      case EncryptionKey.SHA1 => getKeyByteUsingSHA(keyBytes, size, "SHA-1")
      case EncryptionKey.SHA512 => getKeyByteUsingSHA(keyBytes, size, "SHA-512")
      case _ => getKeyByteUsingSHA(keyBytes, size, "SHA-1")
    }
  }

  private def getKeyByteUsingSHA(
      keyByte: Array[Byte],
      size: Int,
      shaMethod: String): Array[Byte] = {
    val key = MessageDigest.getInstance(shaMethod).digest(keyByte)
    java.util.Arrays.copyOf(key, size/8)
  }

  private def cipher(mode: Int, b64secret: String, algo: String): Cipher = {
    val encipher = Cipher.getInstance(algo + "/ECB/PKCS5Padding")
    encipher.init(mode, new SecretKeySpec(decodeBase64(b64secret), algo))

    encipher
  }

  override def operate(df: DataFrame): DataFrame = {
    val columnIDs: Array[Int] =
      (p.getSelectedColumnIdList map (_.asInstanceOf[Int])).toArray

    val result = anonymize(df, getColumnNames(df, columnIDs))
    
    val fieldInfos = (p.getFieldInfoList map (_.asInstanceOf[FieldInfo])).toArray
    Verification(privacy).printAnonymizeResult(df, result, fieldInfos) 
    (result)
  }

}

object EncryptionOperator {
  def apply(o: StreamOperatorInfo): EncryptionOperator = new EncryptionOperator(o)
}
