package main

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.DataFrame

import main.kafka.{Event, EventProducer}

object GithubEventTransformer {


	def main(args: Array[String]): Unit = {
		val fileKey = args(0)
		val hdfsDir = args(1)
		val brokerUrl = args(2)
		val successTopic = args(3)
		val errorTopic = args(4)

		println(args.toSeq)

		val eventProducer = new EventProducer(brokerUrl)

		val conf = new SparkConf().setAppName("GithubEventTransformer")
		val sc = new SparkContext(conf)
		val sqlContext = new HiveContext(sc)

		try{
			//put application logic here
			val df = sqlContext.read.json(hdfsDir).cache
			//processCreateEvent
			processCreateEvent(sqlContext, df)
			//processDeleteEvent
			processDeleteEvent(sqlContext, df)
			//processForkEvent
			processForkEvent(sqlContext, df)

			sendMessage(
				Event(successTopic, fileKey, hdfsDir, System.currentTimeMillis),
				eventProducer
			)
		}catch{
			case e : Throwable => 
				sendMessage(Event(errorTopic, fileKey, 
					e.getMessage, System.currentTimeMillis),
					eventProducer)
		}
	}

	private def processCreateEvent(sqlCtx: HiveContext, df: DataFrame) : Long = {
		println("Running for all create events")
		import sqlCtx.implicits._
		val resultDF = df.where("type = 'CreateEvent'").filter("payload.ref_type != 'tag'").select($"id",$"created_at",$"public".as("is_repo_public"),$"repo.id".as("repo_id"),$"repo.name".as("repo_name"),$"repo.url".as("repo_url"),$"actor.login".as("actor_login"),$"actor.id".as("actor_id"),$"actor.url".as("actor_url"),$"org.id".as("org_id"),$"org.url".as("org_url"),$"org.login".as("org_login"), $"payload.ref_type".as("ref_type"), $"payload.master_branch".as("master_branch"), $"payload.description".as("description"), $"payload.head".as("head"),$"payload.size".as("size")).cache

		println("Number of records is " + (resultDF.count))
		resultDF.write.insertInto("github_archive.create_evt")

		0L
	}

	private def processDeleteEvent(sqlCtx: HiveContext, df: DataFrame) : Long = {
		println("Running for all create events")
		import sqlCtx.implicits._
		

		0L
	}

	private def processForkEvent(sqlCtx: HiveContext, df: DataFrame) : Long = {
		println("Running for all create events")
		import sqlCtx.implicits._
		

		0L
	}

	private def sendMessage(event: Event, kafkaProducer: EventProducer) : Unit = 	
			kafkaProducer.sendMessage(event.instr, event)
}