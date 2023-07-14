package l_11

/** spark-repl (нужно подключать отдельно) => package org.apache.spark.repl.Main  */
/*
  object Main extends Logging {
  ...
  def main(args: Array[String]): Unit = {
  ...
 */

/** spark-core_2.13:3.4.0.jar => package org.apache.spark.deploy */
/*
  object SparkSubmit extends CommandLineUtils with Logging {
  ...
    override def main(args: Array[String]): Unit = {
    ...
 */

/** spark-core_2.13:3.4.0.jar => package org.apache.spark.executor */
/** Класс для описания воркера */
/*
  private[spark] object CoarseGrainedExecutorBackend extends Logging {
  ...
    def main(args: Array[String]): Unit = {
    ...
 */

object SparkDistr {}
