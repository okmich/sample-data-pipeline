package main.kafka

//file-ingestion-complete|2015-01-01-15|hdfs://quickstart.cloudera:8020/user/cloudera/githubarchives/2015-01-01-15|1515840441127
class Event(val instr:String, val key:String, val hdfsPath :String, val ts: Long) {

	override def toString : String = s"$instr|$key|$hdfsPath|$ts"
}


object Event {
	 def apply(instr:String, key:String, hdfsPath :String, ts: Long) : Event = 
	 	new Event(instr,key,hdfsPath,ts)

	 def apply(arg: String) : Event = {
	 	val fields = arg.split("\\|")
	 	new Event(fields(0), fields(1), fields(2), fields(3).toLong)
	 }
}