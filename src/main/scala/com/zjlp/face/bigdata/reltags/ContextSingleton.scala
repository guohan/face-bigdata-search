package com.zjlp.face.bigdata.reltags

import com.zjlp.face.bigdata.utils.Props
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

object MySQLContext {
  private var _instance: SQLContext = _

  def instance(sparkContext: SparkContext = MySparkContext.instance): SQLContext = {
    synchronized {
      if (_instance == null) {
        _instance = new SQLContext(sparkContext)
      }
    }
    _instance
  }
}

object MySparkContext extends Logging {
  private var _instance: SparkContext = _

  private def showSparkConf() = {
    _instance.getConf.getAll.foreach { prop =>
      logInfo(prop.toString())
    }
  }

  private def getSparkConf = {
    val conf = new SparkConf()
    Array(
      "spark.master",
      "spark.app.name",
      "spark.sql.shuffle.partitions",
      "spark.executor.memory",
      "spark.executor.cores",

      "spark.driver.memory",
      "spark.driver.cores",
      "spark.default.parallelism",
      /* "spark.jars",
      "double.filtering",*/
      "es.nodes",
      "es.port",
      "pushdown",
      "strict"
    ).foreach { prop =>
      conf.set(prop, Props.get(prop))
    }
    conf
  }

  def instance() = {
    synchronized {
      if (_instance == null) {
        _instance = new SparkContext(getSparkConf)
        showSparkConf()
      }
    }
    _instance
  }
}
