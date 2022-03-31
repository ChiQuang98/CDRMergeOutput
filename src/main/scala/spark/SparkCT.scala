package spark

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

import java.io.File

object SparkCT {

  val spark = SparkSession.builder()
    .appName("Spark Merge File")
    .getOrCreate()
  def initConfigHadoop():Unit={
    spark.sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    spark.sparkContext.hadoopConfiguration.set("fs.default.name", "hdfs://nameservice1")
    spark.sparkContext.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    spark.sparkContext.hadoopConfiguration.set("dfs.ha.namenodes.nameservice1", "namenode253,namenode428")
    spark.sparkContext.hadoopConfiguration.set("dfs.namenode.rpc-address.nameservice1.namenode253", "mn1-cdp-prod.mobifone.local:8020")
    spark.sparkContext.hadoopConfiguration.set("dfs.namenode.rpc-address.nameservice1.namenode428", "mn2-cdp-prod.mobifone.local:8020")
    spark.sparkContext.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nameservice1",
      "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    spark.sparkContext.hadoopConfiguration.set("hadoop.security.authentication", "kerberos")
    spark.sparkContext.hadoopConfiguration.set("hadoop.rpc.protection", "privacy")
    spark.sparkContext.hadoopConfiguration.set("dfs.namenode.kerberos.principal.pattern", "*")
  }

}
