import java.util.Locale
import scala.language.postfixOps

//"some string" toUpperCase  // err
"some string" toUpperCase;  // ok

"some string" toUpperCase  // ok

"some string" toUpperCase Locale.getDefault()  // ok