package com.ccm.mm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object wordcount {
  def main(args: Array[String]): Unit = {

    //创建sparkConf对象
    //val cfg:SparkConf=new SparkConf().setMaster("192.168.160.124:8080")
    val cfg:SparkConf=new SparkConf().setMaster("local[*]").setAppName("workcount")
    val sc = new SparkContext(cfg)
    val lines: RDD[String]=sc.textFile("in/word.txt")
    val words: RDD[String]=lines.flatMap(_.split(" "))
    val wordone:RDD[(String,Int)]=words.map((_,1))
    val wordsum:RDD[(String,Int)]=wordone.reduceByKey(_+_)
    val result:Array[(String,Int)]=wordsum.collect()
    //println(result)
    result.foreach(println)
  }
}
