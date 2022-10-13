import java.io.File

val fs: String = "file://"
val basePath: String = "/opt/otp/otfs/"
val path: String = "mechfond/well_daily_params"

val absolutePath: String = s"$fs${new File(basePath, path).getAbsolutePath}"