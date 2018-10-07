package io.zhanghao

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计Http的响应码并按照比重进行排序
  *
  * @author zhanghao
  */
object HttpCode {
  def main(args: Array[String]): Unit = {
   //设置sparkConf
    val conf = new SparkConf().setAppName("HttpCode").setMaster("local[*]")
    //设置sparkContext
    val sc = new SparkContext(conf)
    //设置sparkContext的日志级别
    sc.setLogLevel("WARN")
    //设置jobGroup和描述
    sc.setJobGroup("HttpCode", "Http Code statics")
    //从HDFS中读取文本文件
    val data = sc.textFile("data/access_log", 4)
    //打印输出第三个数据
    println("sample:\n" + "-" * 20 + "\n" + data.take(5)(2) + "\n" + "-" * 20)
    //打印输出记录的总数
    println("Records Count:" + data.count())
    /*
    *对data rdd进行map操作,转换为pairRdd(code,1),其中code为响应码位于倒数第二个位置
    *可通过 code.split(" ")(code.split(" ").length - 2 获取到响应码
     */
    val codePairRdd = data.map(code => (code.split(" ")(code.split(" ").length - 2), 1))
    //对pairRdd进行reduceByKey统计结果并进行排序，按照统计结果进行倒排序
    val resultRdd = codePairRdd.reduceByKey(_ + _).sortBy(_._2, ascending = false)
    //打印输出前十个结果
    resultRdd.take(10).foreach(println)
    //TimeUnit.SECONDS.sleep(60)
    //停止sparkContext
    sc.stop()
  }

}
