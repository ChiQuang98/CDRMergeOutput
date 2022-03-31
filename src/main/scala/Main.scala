import org.apache.log4j.{Level, Logger}
import utils.HdfsUtil.{getListFolderInHdfs, initConfig}
import spark.SparkCT
object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = initConfig
    SparkCT.initConfigHadoop()
    val spark = SparkCT.spark
    val arr = getListFolderInHdfs(conf, "/user/ttcntt_icrs/CDR_Project/TempOutput/")
    if (arr.size() < 1) {
      //    if(!true){
      println("QUang1")
      println("Chua co file temp de merge to xlsx")
      System.exit(0)
    } else {

      val pathFileMerge = arr.get(0)
      val temp = pathFileMerge.split("/")
      var nameFile = temp(temp.size - 1)
      println(pathFileMerge,nameFile,"QUANG")
      val df_result = spark.read.parquet(pathFileMerge)
      df_result.write
        .format("com.crealytics.spark.excel") // Or .format("excel") for V2 implementation
        .option("header", "true")
        .mode("overwrite") // Optional, default: overwrite.
        .save("/user/ttcntt_icrs/CDR_Project/etl_output/"+nameFile)

      spark.stop()

    }
  }

}
