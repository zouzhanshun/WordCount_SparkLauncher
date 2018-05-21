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
    launcher.setConf(SparkLauncher.EXECUTOR_CORES, "1")
    launcher.setVerbose(true) //支持SparkSubmit的详细报告。
    //.setConf(SparkLauncher.DEPLOY_MODE,"yarn-cluster")
    launcher.launch
    //TODO 线程状态的查看
    val handle: SparkAppHandle = launcher.startApplication()
    while (handle.getState() != SparkAppHandle.State.FINISHED) {
      //每隔一个小时发送一下程序状态 1000*60*60
      Thread.sleep(60000)
      System.out.println("ApplicationId is: " + handle.getAppId + "current state: " + handle.getState)
      if (handle.getState() == SparkAppHandle.State.FAILED) {
        System.out.println("ApplicationId is: " + handle.getAppId + " job " + SparkAppHandle.State.FAILED)
      }
    }
    if (handle.getState() == SparkAppHandle.State.FINISHED) {
      System.out.println("ApplicationId is: " + handle.getAppId + " job " + SparkAppHandle.State.FINISHED)
    }

  }
}