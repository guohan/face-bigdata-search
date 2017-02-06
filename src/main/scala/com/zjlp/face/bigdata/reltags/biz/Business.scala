package com.zjlp.face.bigdata.reltags.biz

import com.zjlp.face.bigdata.reltags.{EsDaoImpl, MySQLContext}
import com.zjlp.face.bigdata.utils.Props
import org.elasticsearch.spark.rdd.EsSpark
import org.apache.spark.rdd.RDD

/**
 * select distinct user_id from user_hometown where hometown_code in
 * (select hometown_code from user_hometown where user_id = '1201' )
 * and user_id in ('143659','1202','11111')
 */

class Business extends Serializable {
  private val tablePartition = Props.get("spark.sql.shuffle.partitions").toString
  private val sqlContext = MySQLContext.instance()
  private val esIndex = Props.get("es.index.reltags")
  private val maxRosterId: String = sqlContext.read.format("jdbc").options(Map(
    "url" -> Props.get("jdbc_conn"),
    "dbtable" -> s"(select max(rosterId) from view_ofroster ) as max_roster_id",
    "driver" -> Props.get("jdbc_driver")
  )).load().map(a => a(0).toString.toLong).max().toString

  private val maxUserId: String = sqlContext.read.format("jdbc").options(Map(
    "url" -> Props.get("jdbc_conn"),
    "dbtable" -> s"(select max(Id) from user) as max_user_id",
    "driver" -> Props.get("jdbc_driver")
  )).load().map(a => a(0).toString.toLong).max().toString

  /**
   * 获取好友
   */
  def getFriendsRDD: RDD[(String, String)] = {

    sqlContext.read.format("jdbc").options(Map(
      "url" -> Props.get("jdbc_conn"),
      "dbtable" -> " (select rosterID,username,loginAccount,userId from view_ofroster where sub=3 and userId is not null) as friends",
      "driver" -> Props.get("jdbc_driver"),
      "partitionColumn" -> "rosterID",
      "lowerBound" -> "0",
      "upperBound" -> maxRosterId,
      "numPartitions" -> tablePartition
    )).load().registerTempTable("friends")
    sqlContext.sql("cache table friends")
    sqlContext.sql("select distinct loginAccount as login_account,userId as user_id from friends").registerTempTable("userIdMap")
    sqlContext.sql("select distinct user_id,userId from friends inner join userIdMap on username = login_account where user_id != userId").map(r => (r(0).toString, r(1).toString))
  }

  /**
   * 获取校友
   */
  def getAlumniRDD: RDD[(String, String)] = {
    sqlContext.read.format("jdbc").options(Map(
      "url" -> Props.get("jdbc_conn"),
      "dbtable" -> "(select school_id,user_id from education_experience) as edu_exp",
      "driver" -> Props.get("jdbc_driver"),
      "partitionColumn" -> "user_id",
      "lowerBound" -> "0",
      "upperBound" -> maxUserId,
      "numPartitions" -> tablePartition
    )).load().registerTempTable("edu_exp")

    sqlContext.sql("select distinct ee.u1,user_id from (select school_id as s1,user_id as u1 from edu_exp) ee inner join edu_exp  on ee.s1 = school_id where ee.u1 != user_id ").map(r => (r(0).toString, r(1).toString))
  }

  /**
   * 获取同事
   */
  def getColleagueRDD: RDD[(String, String)] = {
    sqlContext.read.format("jdbc").options(Map(
      "url" -> Props.get("jdbc_conn"),
      "dbtable" -> "(select company_id,user_id from career_path) as career",
      "driver" -> Props.get("jdbc_driver"),
      "partitionColumn" -> "user_id",
      "lowerBound" -> "0",
      "upperBound" -> maxUserId,
      "numPartitions" -> tablePartition
    )).load().registerTempTable("career")
    sqlContext.sql("select distinct ca.u1,user_id from (select company_id as c1,user_id as u1 from career) ca inner join career  on ca.c1 = company_id where ca.u1 != user_id ").map(r => (r(0).toString, r(1).toString))
  }

  def readFromES: RDD[(String, String, Int)] = {
    sqlContext.sql(
      "CREATE TEMPORARY TABLE relationTags " +
        "USING org.elasticsearch.spark.sql " +
        s"OPTIONS (resource '${esIndex}', es.read.metadata 'true')")
    sqlContext.sql("select _metadata._type as esType,_metadata._id as esId,tag as esTag from relationTags")
      .map(r => (r(0).toString, r(1).toString, r(2).toString.toInt))
  }

  def compute = {
    readFromES
    val friendsRDD = getFriendsRDD.cache()
    val colleagueRDD = getColleagueRDD.cache()
    val alumniRDD = getAlumniRDD.cache()
    val colleague = colleagueRDD.subtract(friendsRDD).map(t => (t._1, t._2, 2))
    val alumni = alumniRDD.subtract(friendsRDD).subtract(colleagueRDD).map(t => (t._1, t._2, 3))
    val nowRDD = friendsRDD.map(t => (t._1, t._2, 1)).union(colleague).union(alumni).cache()
    val esRDD = readFromES.cache()
    val needDeleteRDD = esRDD.subtract(nowRDD).map(t => (t._1, t._2)).cache()
    val maxPartitionNum = (needDeleteRDD.count() / 5000).toInt + 1
    needDeleteRDD.repartition(maxPartitionNum).foreachPartition {
      p =>
        if (p.hasNext) {
          val esDao = new EsDaoImpl();
          esDao.deleteESData(p);
        }
    }
    val needAddRDD = nowRDD.subtract(esRDD).map(t => (t._2, Map("media_type" -> t._1, "tag" -> t._3)))
    EsSpark.saveToEsWithMeta(needAddRDD, s"${esIndex}/{media_type}", Map("es.mapping.exclude" -> "media_type"))
  }
}
