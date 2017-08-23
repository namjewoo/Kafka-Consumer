package com.knoldus.kafka.consumer


import com.datastax.driver.core._
import com.knoldus.util.KafkaCassandraConfigUtil._
import org.slf4j.LoggerFactory

trait CassandraConnection {

  val logger = LoggerFactory.getLogger(getClass.getName)
  val defaultConsistencyLevel = ConsistencyLevel.valueOf(writeConsistency)
  val cassandraConn: Session = {
    val cluster = new Cluster.Builder().withClusterName("Test Cluster").
      addContactPoints(hosts.toArray: _*).
      withPort(port).
      withQueryOptions(new QueryOptions().setConsistencyLevel(defaultConsistencyLevel)).build
    val session = cluster.connect
    println(s"Keyspace [$cassandraKeyspaces.get(0)]")

    session.execute(s"CREATE KEYSPACE IF NOT EXISTS  ${cassandraKeyspaces.get(0)} WITH REPLICATION = " +
      s"{ 'class' : 'SimpleStrategy', 'replication_factor' : ${replicationFactor} }")
    session.execute(s"USE ${cassandraKeyspaces.get(0)}")
    val query = s"CREATE TABLE IF NOT EXISTS tweets " +
      s"(date text, text text,  " +
      s" PRIMARY KEY (date)) "

    createTables(session, query)
    session
  }

  val cassandraConn1: Session = {
    val cluster = new Cluster.Builder().withClusterName("Test Cluster").
      addContactPoints(hosts.toArray: _*).
      withPort(port).
      withQueryOptions(new QueryOptions().setConsistencyLevel(defaultConsistencyLevel)).build
    val session = cluster.connect
    println(s"Keyspace [$cassandraKeyspaces.get(1)]")

    session.execute(s"CREATE KEYSPACE IF NOT EXISTS  ${cassandraKeyspaces.get(1)} WITH REPLICATION = " +
      s"{ 'class' : 'SimpleStrategy', 'replication_factor' : ${replicationFactor} }")
    session.execute(s"USE ${cassandraKeyspaces.get(1)}")
    val query = s"CREATE TABLE IF NOT EXISTS tweets " +
      s"(date text, text text,   " +
      s" PRIMARY KEY (date)) "
    createTables(session, query)
    session
  }

  val cassandraConn2: Session = {
    val cluster = new Cluster.Builder().withClusterName("Test Cluster").
      addContactPoints(hosts.toArray: _*).
      withPort(port).
      withQueryOptions(new QueryOptions().setConsistencyLevel(defaultConsistencyLevel)).build
    val session = cluster.connect
    println(s"Keyspace [$cassandraKeyspaces.get(2)]")

    session.execute(s"CREATE KEYSPACE IF NOT EXISTS  ${cassandraKeyspaces.get(2)} WITH REPLICATION = " +
      s"{ 'class' : 'SimpleStrategy', 'replication_factor' : ${replicationFactor} }")
    session.execute(s"USE ${cassandraKeyspaces.get(2)}")
    val query = s"CREATE TABLE IF NOT EXISTS tweets " +
      s"(date text, text text, " +
      s" PRIMARY KEY (date)) "
    createTables(session, query)
    session
  }


  def createTables(session: Session, createTableQuery: String): ResultSet = session.execute(createTableQuery)

}

object CassandraConnection extends CassandraConnection
