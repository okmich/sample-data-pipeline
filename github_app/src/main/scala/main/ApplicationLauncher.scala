package main

import kafka.Event
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.launcher.SparkAppHandle

object ApplicationLauncher {

	def launch(jarFile: String, event: Event) : Unit = {
		val command = new SparkLauncher()
		  .setAppResource(jarFile)
		  .setMainClass("main.GithubEventTransformer")
		  .setMaster("yarn")
		  .setVerbose(false)
		  .addAppArgs(event.key, event.hdfsPath, 
		  	"quickstart:9092", "transformation-complete", "transformation-error")

		val appHandle = command.startApplication()
		appHandle.addListener(new SparkAppHandle.Listener{
			def infoChanged(sparkAppHandle : SparkAppHandle) : Unit = {
				println(sparkAppHandle.getState)
			}

			def stateChanged(sparkAppHandle : SparkAppHandle) : Unit = {
				println(sparkAppHandle.getState)
				if ("FINISHED".equals(sparkAppHandle.getState.toString)){
					sparkAppHandle.stop
				}
			}
		})
	}
}