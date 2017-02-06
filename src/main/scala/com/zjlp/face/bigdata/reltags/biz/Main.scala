package com.zjlp.face.bigdata.reltags.biz

object Main {
  def main(args: Array[String]) {
    val beginTime = System.currentTimeMillis()
    val business = new Business()
    business.compute

  /*  //business.
    SparkSession.clearActiveSession()*/
    println(s"共耗时:${(System.currentTimeMillis() - beginTime) / 1000}s")
    System.exit(0)
  }

}
