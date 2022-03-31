import org.apache.log4j.{Level, Logger}
import utils.HdfsUtil.{deleteFileInHdfs, getLastestFolderInHdfs, getListFolderInHdfs, initConfig}
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
      println("Chua co folder temp de merge thanh file xlsx")
      System.exit(0)
    } else {
      val pathFileMerge = getLastestFolderInHdfs(conf,"/user/ttcntt_icrs/CDR_Project/TempOutput/")
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
      //Xoa file temp parquet sau khi merge xong
      deleteFileInHdfs(conf,pathFileMerge)
    }
  }
}
