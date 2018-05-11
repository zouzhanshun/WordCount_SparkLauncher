import java.net.URI

import org.apache.commons.io.IOUtils
import org.apache.spark.launcher.SparkLauncher

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.reflect.io.File

class SparkImportJob(process: Process)(implicit executionContext: ExecutionContext) {

  def exitCode: Future[Int] = Future {
    process.waitFor()
  }

  def stderrIterator: Iterator[String] = {
    Source.fromInputStream(process.getErrorStream).getLines()
  }

  def stdoutIterator: Iterator[String] = {
    Source.fromInputStream(process.getInputStream).getLines()
  }

}

class SparkImportService(sparkHome: String, hadoopConfDir: String, yarnConfDir: String,
                         someCustomSetting: String)(implicit executionContext: ExecutionContext) {

  val jarLocation = "my-job.jar"

  def runSparkJob(): SparkImportJob = {
    val tempFileUri = copyJarToTempFile()

    val env = Map(
      "HADOOP_CONF_DIR" -> hadoopConfDir,
      "YARN_CONF_DIR" -> yarnConfDir,
      "some-custom-setting" -> someCustomSetting
    )

    val process = new SparkLauncher(env.asJava)
      .setSparkHome(sparkHome)
      .setAppResource(tempFileUri.toString)
      .setAppName("My Spark Job")
      .setMainClass("Importer") //Main class in fat JAR
      .setMaster("yarn-client")
      .setConf("spark.driver.memory", "2g")
      .setConf("spark.yarn.queue", "root.imports")
      .setConf("spark.yarn.am.memory", "1g") //This does not actually work.
      .setConf("spark.driver.memory", "1g") //Neither does this.
      .setConf("spark.akka.frameSize", "200")
      .setConf("spark.executor.memory", "8g")
      .setConf("spark.executor.instances", "8")
      .setConf("spark.executor.cores", "12")
      .setConf("spark.default.parallelism", "5000")
      .setVerbose(true)
      .launch()
    new SparkImportJob(process)
  }

  private def copyJarToTempFile(): URI = {
    val tempFile = File.makeTemp("my-job", ".jar")
    val out = tempFile.bufferedOutput()
    try {
      val in = getClass.getClassLoader.getResourceAsStream(jarLocation)
      IOUtils.copy(in, out)
      tempFile.jfile.toURI
    } finally {
      IOUtils.closeQuietly(out)
    }
  }

}