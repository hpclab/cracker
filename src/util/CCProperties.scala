package util

import java.util.Properties
import java.io.InputStream
import java.io.FileInputStream


class CCProperties(algorithmName: String, configurationFile : String) 
{
	val property = new Properties
	
	def load() : CCProperties =
	{
		var input : InputStream = null
 
		input = new FileInputStream(configurationFile);
 
		property.load(input);
		
		this
	}
	
	def get(data : String, default : String) =
	{
		property.getProperty(data, default)
	}
	
	def getBoolean(data : String, default : Boolean) =
	{
		get(data, default.toString).toBoolean
	}
	
	def getImmutable : CCPropertiesImmutable =
	{
		val dataset = get("dataset", "")
		val jarPath = get("jarPath", "")
		val sparkMaster = get("sparkMaster", "local[2]")
		val sparkExecutorMemory = get("sparkExecutorMemory", "14g")
		val sparkPartition = get("sparkPartition", "32").toInt
		val sparkBlockManagerSlaveTimeoutMs= get("sparkBlockManagerSlaveTimeoutMs", "45000")
		var separator = get("edgelistSeparator", "space")
		if(separator.equals("space")) separator = " "
		val printMessageStat = get("printMessageStat", "false").toBoolean
		val printCCDistribution = get("printCCDistribution", "false").toBoolean
		val printAll = get("printAll", "false").toBoolean
		val customColumnValue = get("customColumnValue", "")
		val algorithmNameFromConfiguration = get("algorithmName", algorithmName)
		
		new CCPropertiesImmutable(algorithmNameFromConfiguration, dataset, jarPath, sparkMaster, sparkPartition, sparkExecutorMemory, sparkBlockManagerSlaveTimeoutMs, separator, printMessageStat, printCCDistribution, printAll, customColumnValue)
	}
}