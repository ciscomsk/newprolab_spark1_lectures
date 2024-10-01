import java.util.Locale
import scala.language.postfixOps

//"some string" toUpperCase // err
"some string" toUpperCase; // ok

"some string" toUpperCase // ok

"some string" toUpperCase Locale.getDefault() // ok

//Some("a".toInt) // java.lang.NumberFormatException

//Nil.head // java.util.NoSuchElementException
//Nil.tail // java.lang.UnsupportedOperationException