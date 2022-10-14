def xo(str: String): Boolean = {
  val lowerCaseStr: String = str.toLowerCase()
  lowerCaseStr.count(_ == 'h') == lowerCaseStr.count(_ =='o')
}