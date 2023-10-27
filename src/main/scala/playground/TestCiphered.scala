package playground

import org.apache.commons.codec.binary.Base32
import org.bouncycastle.crypto.digests.SHA3Digest
import org.bouncycastle.crypto.macs.HMac
import org.bouncycastle.crypto.params.KeyParameter

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Base64

object TestCiphered extends App {
  val key: String = "wbgcjl/JesH/VDZtuzNmGIdDzpc78VsLxT1yxnKrrvg="
  val res: Array[Byte] = Base64.getDecoder.decode(key)

//  println(res.mkString("Array(", ", ", ")"))
//  println(new String(res))

  val cipheredPkg: String = "2V4WHFRHY7X2F6YCPFA".substring(1)
//  println(cipheredPkg)


  var addedCipheredPkg = cipheredPkg
  while (addedCipheredPkg.length % 8 != 0) {
    addedCipheredPkg += "="
  }
  println(addedCipheredPkg)

  val eqToAppend: Int = 8 - (cipheredPkg.length % 8)
  println(eqToAppend)

  val appendedStr: String = cipheredPkg + ("=" * eqToAppend)
  println(appendedStr)


  val codec32: Base32 = new Base32
  val ascii: Array[Byte] = codec32.decode(cipheredPkg)

//  println(new String(ascii))

  val date = "2017-06-22 12:00"

  val dateTime: LocalDateTime = LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"))
//  println(dateTime)
  println(dateTime.minusHours(3L))
  println(dateTime.toLocalDate.toString)

  val dateStr: String = dateTime.toLocalDate.toString
//  println(dateStr)
//  println(new String(dateStr.getBytes))
  val dateBytes: Array[Byte] = dateStr.getBytes
//  println(dateBytes.mkString("Array(", ", ", ")"))


  res
  ascii
  dateBytes

//  val arr = Array(1, 2, 3, 4).take(3)
//  println(arr.mkString("Array(", ", ", ")"))

  val iv1: Array[Byte] = ascii.take(3)
//  println(new String(iv1))

  val arrSum: Array[Byte] = iv1 ++ dateBytes
//  println(new String(arrSum))

//  val coded1: Mac = HmacUtils.getInitializedMac(HmacAlgorithms.HMAC_SHA_256, res)
//  val finRes1 = coded1.doFinal(arrSum)
//  println(new String(finRes1))
//
//  val coded2: Mac = HmacUtils.getInitializedMac(HmacAlgorithms.HMAC_SHA_256, res)
//  coded2.update(arrSum)
//  val finRes2 = coded2.doFinal()
//  println(new String(finRes2))


//  val HMAC_ALGO: String = "HmacSHA3-256"
//  val signer = Mac.getInstance(HMAC_ALGO)
//  private val sk = new SecretKeySpec(res, HMAC_ALGO)
//  signer.init(sk)
//  val finRes = signer.doFinal(arrSum)
//  println(new String(finRes))


  val dg: SHA3Digest = new SHA3Digest(256)
  val hMac = new HMac(dg)
  hMac.init(new KeyParameter(res))
  hMac.update(arrSum, 0, arrSum.length)
  val hmacOut: Array[Byte] = new Array[Byte](hMac.getMacSize)
  hMac.doFinal(hmacOut, 0)
//  println(new String(hmacOut))

  val arr = Array(1, 2, 3, 4).drop(3)
//  println(arr.mkString("Array(", ", ", ")"))

  val ciphered: Array[Byte] = ascii.drop(3)
//  println(new String(ciphered))
//  println(ciphered.length)

//  val coll =
//    (1 to ciphered.length).map(idx => )
//
//  ciphered
//    .zipWithIndex
//    .map { case (byte, idx) =>
////      byte ^ hmacOut(idx)
//      byte ^ coll(idx)
//    }

//  val endless: Iterator[Long] = Iterator.iterate(1L)(_ + 1)
//  println(endless.next())

//  def func(v1: Int, v2: Int): Int = v1 + v2
//
//  Iterator.continually(func(1, 2))
//  println(Iterator.continually(func(1, 2)).next())
//  println(Iterator.continually(func(1, 2)).next())

//  ciphered.map()

  println(List(10, 20, 30).zipWithIndex)

}