package com.knoldus.kafka.consumer


import java.util.Properties

import akka.actor._
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerIterator, ConsumerTimeoutException}
import kafka.consumer._
import org.slf4j.LoggerFactory
import java.util.HashMap

import kafka.serializer.StringDecoder
import kafka.utils.VerifiableProperties
import scala.collection.mutable


class KafkaConsumerSample {
  val logger = LoggerFactory.getLogger(this.getClass)

  private val props = new Properties
  props.put("group.id", "batch_consumer")
  props.put("bootstrap.servers", "broker.kafka.l4lb.thisdcos.directory:9092")
  props.put("zookeeper.connect", "master.mesos:2181/dcos-service-kafka")
  props.put("enable.auto.commit", "true")
  props.put("auto.offset.reset", "smallest")
  props.put("consumer.timeout.ms", "5000")
  props.put("auto.commit.interval.ms", "1000")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  private val noOfStreams =1
  private val batchSize = 100
  private val topic = "tweets"
  private val consumerConnector = Consumer.create(new ConsumerConfig(props))
  private val iterator: ConsumerIterator[Array[Byte], Array[Byte]] = consumerConnector.createMessageStreams(Map(topic -> noOfStreams)).mapValues(_.head)(topic).iterator()

  /*
  val decoder = new StringDecoder(new VerifiableProperties())
  var stream =  consumerConnector.createMessageStreams(Map(topic -> 10),decoder, decoder).get(topic).get(0)
  //var iterator = stream.iterator()
  var streams:Map[String, KafkaStream[String,String]] = Map.empty
  streams += (topic -> stream)
  */
  def read =
    try {
      println(s"read Start:::::::::::::::::")

      if (iterator.hasNext()) {
         println(s"Got message   ::::::::::::::::::: ")

        //var buffer = iterator.next().message()

        //var message  = new String(buffer)

        //println(s"recv message[ $message ] ")

        readBatchFromTopic(topic, iterator)
      }
      else
        println("$$$$$ no data::::::::::")

    } catch {
      case timeOutEx: ConsumerTimeoutException =>
        println("$$$Getting time out  when reading message", timeOutEx)
      case ex: Exception =>
        println(s"Not getting message from ", ex)
    }


  private def readBatchFromTopic(topic: String, iterator: ConsumerIterator[Array[Byte], Array[Byte]]) = {
    var batch = List.empty[String]
    while (hasNext(iterator) && batch.size < batchSize) {
      batch = batch :+ (new String(iterator.next().message()))
    }
    if (batch.isEmpty) throw new IllegalArgumentException(s"$topic is  empty")else{ CassandraOperation.insertTweets(batch)}
    println(s"consumed batch into cassandra ::::::::::: ${batch.length}         ${batch.apply(0)}")
  }

  private def hasNext(it: ConsumerIterator[Array[Byte], Array[Byte]]): Boolean =
    try {
      it.hasNext()
    }catch {
      case timeOutEx: ConsumerTimeoutException =>
        println("Getting time out  when reading message :::::::::::::: ")
        false
      case ex: Exception =>
        println("Getting error when reading message :::::::::::::::::  ", ex)
        false
    }

}

import scala.concurrent.duration.DurationInt

case object GiveMeWork

class KafkaMessageConsumer(consumer: KafkaConsumerSample) extends Actor  {

  implicit val dispatcher = context.dispatcher

  val initialDelay = 1000 milli
  val interval = 1 seconds


  context.system.scheduler.schedule(initialDelay, interval, self, GiveMeWork)

  def receive: PartialFunction[Any, Unit] = {

    case GiveMeWork => consumer.read
  }

}

object CassandraKafkaConsumer extends App {

  val actorSystem = ActorSystem("KafkaActorSystem")

  val consumer = actorSystem.actorOf(Props(new KafkaMessageConsumer(new KafkaConsumerSample)))

  consumer ! GiveMeWork

}
