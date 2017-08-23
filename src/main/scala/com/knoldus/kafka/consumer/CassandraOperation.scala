package com.knoldus.kafka.consumer

import java.util.Date

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

case class TwitterUser(date: String, text: String)

case class Response(numberOfUser: Long, data: List[TwitterUser])

object CassandraOperation extends CassandraConnection {

  def insertTweets(listJson: List[String], tableName: String = "collect.tweets") = {
    println(s":::::::::::::::::::::::::::: batch inserted[$tableName]")
    listJson.map(json => cassandraConn.execute(s"INSERT INTO $tableName JSON '$json'"))
  }

  def findTwitterUsers(minute: Long, second: Long, tableName: String = "tweets"): Response = {
    val batchInterval = System.currentTimeMillis() - minute * 60 * 1000
    val realTimeInterval = System.currentTimeMillis() - second * 1000
    val batchViewResult = cassandraConn.execute(s"select * from batch_view.friendcountview where createdat >= ${batchInterval} allow filtering;").all().toList
    val realTimeViewResult = cassandraConn.execute(s"select * from realtime_view.friendcountview where createdat >= ${realTimeInterval} allow filtering;").all().toList
    val twitterUsers: ListBuffer[TwitterUser] = ListBuffer()
    batchViewResult.map { row =>
      twitterUsers += TwitterUser(row.getString("date"), row.getString("text"))
    }
    realTimeViewResult.map { row =>
      twitterUsers +=  TwitterUser(row.getString("date"), row.getString("text"))
    }

    Response(twitterUsers.length, twitterUsers.toList)
  }

}
