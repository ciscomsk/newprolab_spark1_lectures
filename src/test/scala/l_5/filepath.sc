import java.io.File

val fs = "file://"
val basePath = "/opt/otp/otfs/"
val path = "mechfond/well_daily_params"

val absolutePath = s"$fs${new File(basePath, path).getAbsolutePath}"