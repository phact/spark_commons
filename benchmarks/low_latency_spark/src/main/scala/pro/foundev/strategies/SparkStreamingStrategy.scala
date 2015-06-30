/*
 *  Copyright 2015 Foundational Development
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package pro.foundev.strategies

import com.datastax.bdp.spark.DseStreamingContext
import com.datastax.spark.connector.cql.CassandraConnector
import com.rabbitmq.client.{ConnectionFactory, QueueingConsumer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import pro.foundev.calculations.{CheapLogCalculator, LogCalculator, LogCalculatorImpl}
import pro.foundev.cassandra.CassandraRepository
import pro.foundev.dto.IpLog
import pro.foundev.messaging.{AbstractQueueReceiver, RabbitMQPublisher}
import pro.foundev.random.BenchmarkSeeding


class SparkStreamingStrategy(master:String,
                             logCalculator: LogCalculator,
                             benchmarkSeeding: BenchmarkSeeding,
                              receiver: AbstractQueueReceiver)
  extends BenchmarkStrategy(master) with Serializable{
    println("configured master for spark streaming is " + master)
  val sc: StreamingContext = initStream
  val topic = "benchmark"
  val mqUrl = "tcp://"+getMaster + ":10050"
  val factory = new ConnectionFactory()
  factory.setHost(master)
  val publishConnection = factory.newConnection()
  val publishChannel = publishConnection.createChannel()

  //queue names
  val queueNameForPartBenchmark = "benchmark_part_queue"
  val queueNameForReportSession = "report_session_queue"

  // exchange names
  val benchmarkPartExchange = "benchmark_part_queries"
  val reportSessionExchange = "report_session"

  //queue declaration
  publishChannel.queueDeclare(queueNameForPartBenchmark, true, false, false, null).getQueue
  publishChannel.queueDeclare(queueNameForReportSession, true, false, false, null).getQueue

  //declare exchanges
  publishChannel.exchangeDeclare(benchmarkPartExchange, "fanout")
  publishChannel.exchangeDeclare(reportSessionExchange, "fanout")

  //bind queues
  publishChannel.queueBind(queueNameForPartBenchmark, benchmarkPartExchange, "")
  publishChannel.queueBind(queueNameForReportSession, reportSessionExchange, "")

  //consumers
  val reportSessionConsumer = new QueueingConsumer(publishChannel)

  //wire up consumers
  publishChannel.basicConsume(queueNameForReportSession, true, reportSessionConsumer)

  //initialize spark streaming thread
  val thread: Thread = new Thread(startStreaming)
  thread.start()
  Thread.sleep(1000)

  //warm up cache
  executeComputationOnRangeOfPartitions()
  executeComputationOnPartitionFoundOn2i()

  def close()={
    publishChannel.close()
    publishConnection.close()
  }
  private def initStream: StreamingContext = {
    val level: Level = Level.ERROR
    Logger.getLogger("org").setLevel(level)
    Logger.getLogger("akka").setLevel(level)
    val host = getHost(master)
    val sparkConf = new SparkConf()
      .set("driver.host", host)
      .set("spark.driver.allowMultipleContexts", "true")
      .setAppName("My application")
      .set("spark.cassandra.connection.host",host)
      .set("spark.cassandra.output.concurrent.writes","1")
      .set("spark.cassandra.output.batch.size.rows", "1")
      .set("spark.cassandra.input.split.size", "10000")
      .set("spark.cassandra.input.page.row.size", "10")
      .setJars(Array("target/scala-2.10/interactive_spark_benchmarks-assembly.jar"))
      .setMaster(getMasterUrl())
    val sc = DseStreamingContext(sparkConf, Milliseconds(100))
    val repo = new CassandraRepository
    val connector = CassandraConnector(sparkConf)
    receiver.setHost(master)
    receiver.setQueueName("benchmark_part_queue")
    val logs: DStream[String] = sc.receiverStream(receiver)
    val cachedMessages = logs.cache()
    val states = cachedMessages.filter(m=>m.forall(!_.isDigit))
    val localReportSessionExchange = "report_session"
    val stateLogs: DStream[IpLog] = states.flatMap(state => repo.getLogsFromRandom2iUsingConnector(state, connector) )
    stateLogs.foreachRDD(rdd=>{
      if(rdd.partitions.length == 0 || rdd.mapPartitions(it => Iterator(!it.hasNext)).reduce(_&&_)) {
      }else {
        val sessionReport = new LogCalculatorImpl().sessionReport(rdd)
        val p = new RabbitMQPublisher(host, localReportSessionExchange)
        p.publish(sessionReport.toString())
      }
    })
    val idsToFind = cachedMessages.filter(m=>m.forall(_.isDigit))
    idsToFind.map(x=>x.toInt)
    .map(id=>repo.getLogForId(id, connector))
    .foreachRDD(rdd=>{
           if(rdd.partitions.length == 0 || rdd.mapPartitions(it => Iterator(!it.hasNext)).reduce(_&&_)) {
          }else{
            val s = new CheapLogCalculator().totalUrls(rdd)
            val p = new RabbitMQPublisher(host, localReportSessionExchange )
            p.publish(s.toString)
          }
    })
    sc
  }
  private def startStreaming(): Runnable = {
    new Runnable {
      override def run(): Unit = {
        sc.start()
        sc.awaitTermination()
      }
    }
  }
  def shutDown(): Unit ={
    sc.stop()
  }
  override protected def getStrategyName: String = { "Streaming Strategy"}

  override protected def executeComputationOnRangeOfPartitions(): Unit = {
    val ids = benchmarkSeeding.ids.map(x=>x.toString).toList
    submitMessages(ids)
    println("Report in the driver is " +getResult())
  }


  override protected def executeComputationOnPartitionFoundOn2i(): Unit = {
    val state = benchmarkSeeding.state
    submitMessages(List[String](state))
    println(getResult())
  }


  override protected def executeCheapComputationOnSinglePartition(): Unit = {
    val id = benchmarkSeeding.randId.toString
    submitMessages(List[String](id))
    println(getResult())
  }

  private def submitMessages(messages: List[String]): Unit ={
    messages.foreach(message=>{
      println("Publishing message " + message)
      publishChannel.basicPublish(benchmarkPartExchange, "", null, message.getBytes())
    })
  }

  private def getResult():String = {
    val message = reportSessionConsumer.nextDelivery(60000)
    if(message==null) {
      throw new RuntimeException("no message came in within 60 seconds")
    }
    val result = new String(message.getBody, "UTF-8")
    println("Listing result " + result)
    result
  }

}
