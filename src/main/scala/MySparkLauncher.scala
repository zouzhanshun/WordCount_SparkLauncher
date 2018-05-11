import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}


object MySparkLauncher {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val launcher = new SparkLauncher()
    launcher.setSparkHome("/opt/apps/spark-2.3.0-bin-2.6.0-cdh5.7.0")
    launcher.setAppResource("/home/zouzhanshun/spark-wordcount-1.0-SNAPSHOT.jar")
    //.setAppResource("D:\\workspace_idea\\spark-wordcount\\target\\spark-wordcount-1.0-SNAPSHOT.jar")
    launcher.setMainClass("SimpleApp")
    //.setMaster("spark://JY2TEHDP01:7077")
    launcher.setMaster("yarn-cluster")
    //.setMaster("local[2]")
    launcher.setAppName("spark launcher")
    launcher.setConf(SparkLauncher.DRIVER_MEMORY, "2G")
    launcher.setConf(SparkLauncher.EXECUTOR_MEMORY, "2G")
    launcher.setConf(SparkLauncher.EXECUTOR_CORES, "2")
    launcher.setVerbose(true) //支持SparkSubmit的详细报告。
    //.setConf(SparkLauncher.DEPLOY_MODE,"yarn-cluster")
    launcher.launch
    val handle: SparkAppHandle = launcher.startApplication()
    while (handle.getState() != SparkAppHandle.State.FINISHED) {
      Thread.sleep(1000L)
      System.out.println("applicationId is: " + handle.getAppId)
      System.out.println("current state: " + handle.getState)
      //handle.stop();
    }

    //    println("=============start")
    //spark.waitFor
    //    println("spark.waitFor(): " + spark.waitFor())
    //    sys.exit(spark.waitFor())
  }
}