package cracker

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CCPropertiesImmutable
import util.CCUtil

@serializable
class CrackerStats(property : CCPropertiesImmutable, util : CCUtil, spark : SparkContext) {
    
    val reduceInputMessageNumberAccumulator = spark.accumulator(0L)
	val reduceInputSizeAccumulator = spark.accumulator(0L)

    def printSimplification(step : Int, rdd : RDD[(Long, CrackerTreeMessageIdentification)]) =
    {
        if (property.printMessageStat) 
        {
        	if(rdd.count > 0)
        		util.printSimplification(step, rdd.count, rdd.map(t=> t._2.neigh.size.toLong).sum, rdd.map(t=> t._2.neigh.size).max)
        	else
        		util.printSimplification(step, 0, 0, 0)
		}
        if(property.printAll)
		{
			printGraph(util, step, "INPUT_BLUE", rdd)
		}
    }
    
    def printSimplificationCCF(step : Int, rdd : RDD[(Long, Iterable[Long])]) =
    {
        if (property.printMessageStat) 
        {
            val count = rdd.count 
        	if(count > 0)
        		util.printSimplification(step, count, rdd.map(t=> t._2.size.toLong).sum, rdd.map(t=> t._2.size).max)
        	else
        		util.printSimplification(step, 0, 0, 0)
		}
//        if(property.printAll)
//		{
//			printGraph(util, step, "INPUT_BLUE", rdd)
//		}
    }
    
    def printMessageStats[A<%CrackerMessageSize](step : Int, rdd : RDD[(Long, A)]) =
    {
        if (property.printMessageStat) {
			val previousMessageSize = reduceInputSizeAccumulator.value
			val previousMessageNumber = reduceInputMessageNumberAccumulator.value
			
			rdd.foreach(t => reduceInputSizeAccumulator += t._2.getMessageSize)
			reduceInputMessageNumberAccumulator += rdd.count
			
			util.printMessageStep(step, reduceInputMessageNumberAccumulator.value - previousMessageNumber, reduceInputSizeAccumulator.value - previousMessageSize)
		}
    }
    
    def printGraph(util : CCUtil, step : Int, description : String,  g : RDD[(Long, CrackerTreeMessageIdentification)]) =
	{
		util.io.printToFile("graph.txt", "STEP "+step+"\t["+description+"]\t"+ g.map(t => "{"+t._1 +" "+ t._2.toString+"} ").reduce{case (a,b) => a+b}+"\n")
	}
}