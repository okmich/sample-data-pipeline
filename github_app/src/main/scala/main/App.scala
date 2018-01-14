package main

import main.kafka.EventConsumer

object App {

	def main(args: Array[String]) : Unit = {
		if (args.isEmpty){
			println("Please enter the full path to your application jar file")
			System.exit(-1)
		}
		var appJarFile = args(0)

		val eventConsumer = new EventConsumer(
			"quickstart.cloudera:9092", "file-ingestion-complete")

		eventConsumer.start(appJarFile)
		
	}
}