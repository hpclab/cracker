package alternating

import util.CCPropertiesImmutable
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CCUtil
import cracker.CrackerStats

@serializable
class AlternatingStats(property : CCPropertiesImmutable, util : CCUtil, spark : SparkContext) {

//	val crackerStats = new CrackerStats(property, util, spark)
	val reduceInputMessageNumberAccumulator = spark.accumulator(0L)
	val reduceInputSizeAccumulator = spark.accumulator(0L)
			
	def printSimplificationAlternating(step : Int, rdd : RDD[(Long, Set[Long])]) =
    {
        if (property.printMessageStat) 
        {
			util.printSimplification(step, rdd.count, rdd.map(t=>t._2.size.toLong).reduce{case(a,b)=>a+b}, rdd.map(t=>t._2.size).max)
		}
//        if(property.printAll)
//		{
//			printGraph(util, step, "INPUT_BLUE", rdd)
//		}
    }
	
	def countMessage(ret : RDD[(Long, Set[Long])], step : Int) =
	{
		if (property.printMessageStat) {
			val previousMessageSize = reduceInputSizeAccumulator.value
			val previousMessageNumber = reduceInputMessageNumberAccumulator.value
			
			ret.foreach(t => reduceInputSizeAccumulator += t._2.size + 1)
			reduceInputMessageNumberAccumulator += ret.count
			
			util.printMessageStep(step, reduceInputMessageNumberAccumulator.value - previousMessageNumber, reduceInputSizeAccumulator.value - previousMessageSize)
		}
	}
}