package playground

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.Base64

object TestApp extends App {
//  val path: Path = Paths.get("/home/mike/Downloads/photo_2022-08-18-10.21.55.jpg")
//  val byteArr: Array[Byte] = Files.readAllBytes(path)
////  println(byteArr.mkString("Array(", ", ", ")"))
//
//  val b64String: String = Base64.getEncoder.encodeToString(byteArr)
//  println(b64String)
//  val saveString = Paths.get("/home/mike/Downloads/photo.txt")
//  Files.write(saveString, b64String.getBytes)
//
//  val decodedByteArr: Array[Byte] = Base64.getDecoder.decode(b64String)
//
//  val saveJpg = Paths.get("/home/mike/Downloads/photo_saved.jpg")
//  Files.write(saveJpg, decodedByteArr)

  val path: Path = Paths.get("/home/mike/Downloads/39c25893-18ae-4b1a-bc32-4986aee58b66")
  val byteArr: Array[Byte] = Files.readAllBytes(path)
//  println(new String(byteArr, StandardCharsets.UTF_8))
  val decodedByteArr: Array[Byte] = Base64.getDecoder.decode(new String(byteArr, StandardCharsets.UTF_8).getBytes())
//
//  val saveJpg = Paths.get("/home/mike/Downloads/39c25893-18ae-4b1a-bc32-4986aee58b66.jpg")
//  Files.write(saveJpg, decodedByteArr)

}
