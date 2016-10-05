package alternating

import java.io.FileWriter
import scala.collection.immutable.TreeSet
import scala.collection.mutable.ListBuffer
import org.apache.spark.Accumulator
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CCUtil
import util.CCProperties
import cracker.CrackerStats

object AlternatingMain {
	
	
	def main(args : Array[String]) : Unit =
		{
			val timeBegin = System.currentTimeMillis()

            val property = new CCProperties("ALTERNATING", args(0)).load.getImmutable
            
            val util = new CCUtil(property)
			
            val spark = util.getSparkContext()
            val alternating = new AlternatingAlgorithm
            val stats = new AlternatingStats(property, util, spark)
            
            val timeSparkLoaded = System.currentTimeMillis()
            val file = spark.textFile( property.dataset , property.sparkPartition)

            util.io.printFileStart(property.appName)
            
//            val (parsedData, fusedData) = util.loadVertexEdgeFile(file)
            val (parsedData, fusedData) = util.loadEdgeFromFile(file)
            
			var ret = fusedData.flatMap(alternating.generateInitialEdge).reduceByKey(alternating.reduceMessageByKey).cache //.map( item => ( item._1, new CcfMessage( toTreeSet(item._2.toSet), false) ) )
			ret.count
			
			val timeDataLoaded = System.currentTimeMillis()

			var control = false;
			var step = 0

			val reduceInputMessageNumberAccumulator = spark.accumulator(0L)
			val reduceInputSizeAccumulator = spark.accumulator(0L)
			
			var previousRDDForConvergence = ret.map(t => (t._1, Math.min(t._2.min, t._1))).cache
			previousRDDForConvergence.count

			while (!control) {
				val timeStepStart = System.currentTimeMillis()

				stats.printSimplificationAlternating(step, ret)
				ret = ret.flatMap(item => alternating.largeStarMap(item))

				stats.countMessage(ret, step)

				ret = ret.reduceByKey(alternating.reduceMessageByKey).flatMap(alternating.largeStarReduce)
				
				stats.countMessage(ret, step)
				
				ret = ret.reduceByKey(alternating.reduceMessageByKey).cache
				
				val timeStepLarge = System.currentTimeMillis()
				util.io.printTime(timeStepStart, timeStepLarge, "large")
				util.printTimeStep(step, timeStepLarge-timeStepStart)
				stats.printSimplificationAlternating(step+1, ret)

				ret = ret.flatMap(alternating.smallStarReduce)
				
				stats.countMessage(ret, step)
				
				ret = ret.reduceByKey(alternating.reduceMessageByKey).cache

				val rddForConvergence = ret.map(t => (t._1, Math.min(t._2.min, t._1))).cache
				control = previousRDDForConvergence.leftOuterJoin(rddForConvergence).map(t => if(t._2._2.isDefined) t._2._1 == t._2._2.get else false).cache.reduce{case(a,b) => a&&b}
				previousRDDForConvergence = rddForConvergence
				
				val timeStepSmall = System.currentTimeMillis()

				step = step + 3
				util.io.printTime(timeStepLarge, timeStepSmall, "small")
				util.printTimeStep(step+1, timeStepSmall-timeStepLarge)
			}

			val timeEnd = System.currentTimeMillis()
			
			util.testEnded(	ret.map(t=> (t._2.min, 1)).reduceByKey{case (a,b)=> a+b}.map(t=>(t._1, t._2)), 
            				step, 
            				timeBegin, 
            				timeEnd, 
            				timeSparkLoaded, 
            				timeDataLoaded, 
            				reduceInputMessageNumberAccumulator.value, 
            				reduceInputSizeAccumulator.value)
			
		}
}
