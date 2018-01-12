
import org.apache.spark.launcher.SparkLauncher

object ApplicationLauncher {

	def launch : Unit = {
		val command = new SparkLauncher()
		  .setAppResource("SparkPi")
		  .setMainClass("GithubEventTransformer")
		  .setMaster("yarn")
		  .setVerbose(true)
		  .addAppArgs("")

		val appHandle = command.startApplication()
	}
}