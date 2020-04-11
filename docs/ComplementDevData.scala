package com.bawei.spark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object ComplementDevData {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("complementDevData")
      .getOrCreate()

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","node4:2181")
    val scan = new Scan
    val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray)
    hbaseConf.set(TableInputFormat.SCAN,scanToString)

    val devStaticsData: RDD[(ImmutableBytesWritable, Result)] = spark.sparkContext.newAPIHadoopRDD(hbaseConf
      , classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable]
      , classOf[Result])


    import spark.implicits._
    val devDataDF: DataFrame = devStaticsData.map(x => {
      val endTime = Bytes.toLong(x._1.get(), x._1.getOffset, x._1.getLength)
      val kaijilv = Bytes.toDouble(x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("kaijilv")))
      val guzhanglv = Bytes.toDouble(x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("guzhanglv")))
      val devamount = Bytes.toLong(x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("devamount")))
      val kaijishu = Bytes.toLong(x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("kaijishu")))
      val guzhangshu = Bytes.toLong(x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("guzhangshu")))
      val windowStart = Bytes.toLong(x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("windowstart")))
      (kaijilv, guzhanglv, kaijishu, guzhangshu, devamount, windowStart, endTime)
    }).toDF("kaijilv", "guzhanglv", "kaijishu", "guzhangshu", "devamount", "startTime", "endTime")

    val text = spark.sparkContext.textFile("hdfs://......")

    val maped: RDD[(Long, (String, String, String, String, String, Long))] = text.map(line => {
      val arr = line.split(",")
      (arr(5).toLong, (arr(0), arr(1), arr(2), arr(3), arr(4), arr(5).toLong))
    })

    val mapValued: RDD[(Long, (Long, Long))] = maped.groupByKey().mapValues(it => {
      var kaijishu = 0L;
      var guzhangshu = 0L;
      it.foreach(x => {
        if ("2".eq(x._4)) {
          kaijishu = kaijishu + 1;
          guzhangshu = guzhangshu + 1;
        } else {
          kaijishu = kaijishu + 1;
        }
      })
      (kaijishu, guzhangshu)
    })

    val bushuDF: DataFrame = mapValued.map(x => {
      (x._1, x._2._1, x._2._1)
    }).toDF("devdataTime", "newKaijishu", "newGuzhangshu")


    val res: DataFrame = bushuDF.join(devDataDF, $"devdataTime" >= $"startTime" and $"devdataTime" < $"endTime", "left_outer")

    res.show()
    res.rdd.map(row => {

    })

  }

}
