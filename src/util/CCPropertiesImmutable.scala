package util

class CCPropertiesImmutable(val algorithmName : String, 
							val dataset : String, 
							val jarPath : String, 
							val sparkMaster : String,
							val sparkPartition : Int,
							val sparkExecutorMemory : String, 
							val sparkBlockManagerSlaveTimeoutMs : String,
							val separator : String,
							val printMessageStat : Boolean,
							val printCCDistribution : Boolean,
							val printAll : Boolean,
							val customColumnValue : String) extends Serializable
{
	def appName = algorithmName+"_"+dataset
}