package alternatingOptimized

import scala.collection.mutable.ListBuffer
import org.apache.spark.Accumulator
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import cracker.CrackerStats
import util.CCProperties
import util.CCUtil
import com.google.common.base.Joiner
import java.io.FileWriter
import alternating.AlternatingAlgorithm
import alternating.AlternatingStats

object AlternatingOptimizedMain {


	def main(args : Array[String]) : Unit =
		{
			val timeBegin = System.currentTimeMillis()

            val property = new CCProperties("ALTERNATINGOPTIMIZED", args(0)).load.getImmutable
            
            val util = new CCUtil(property)
			
            val spark = util.getSparkContext()
            val alternating = new AlternatingAlgorithm
            val stats = new AlternatingStats(property, util, spark)
            
            val timeSparkLoaded = System.currentTimeMillis()
            val file = spark.textFile( property.dataset , property.sparkPartition)

            util.io.printFileStart(property.appName)
            util.io.printStat(property.vertexIdMultiplier, "idMultiplier")
            
//            val (parsedData, fusedData) = util.loadVertexEdgeFile(file)
            val (parsedData, fusedData) = util.loadEdgeFromFile(file)
            
//			var ret = fusedData.flatMap(alternating.generateInitialEdge).reduceByKey(alternating.reduceMessageByKey).cache //.map( item => ( item._1, new CcfMessage( toTreeSet(item._2.toSet), false) ) )
			var ret = fusedData.map(t => (t._1, t._2.toSet)).cache
			
			val timeDataLoaded = System.currentTimeMillis()
			ret.count
			
			var control = false;
			var step = 0

			var previousRDDForConvergence = ret.map(t => (t._1, Math.min(t._2.min, t._1))).cache
			previousRDDForConvergence.count
			
			while (!control) {
				val timeStepStart = System.currentTimeMillis()

				stats.printSimplificationAlternating(step, ret)
				var previousRet = ret
				ret = ret.flatMap(item => alternating.largeStarMapOptimized(item, property.vertexIdMultiplier)).cache
				
				ret.first()
				previousRet.unpersist()
				
				stats.countMessage(ret, step + 1)
				
				previousRet = ret
				ret = ret.reduceByKey(alternating.reduceMessageByKey).flatMap(item => alternating.largeStarReduceOptimized(item))
				
				stats.countMessage(ret, step + 2)
				
				var previousRet2 = ret
				ret = ret.reduceByKey(alternating.reduceMessageByKey).cache
				
				ret.first()
				previousRet.unpersist()
				previousRet2.unpersist()
				
				val timeStepLarge = System.currentTimeMillis()
				util.io.printTime(timeStepStart, timeStepLarge, "large")
				util.printTimeStep(step, timeStepLarge-timeStepStart)
				stats.printSimplificationAlternating(step+1, ret)

				previousRet = ret
				ret = ret.flatMap(alternating.smallStarReduce)
				
				stats.countMessage(ret, step + 3)
				
				previousRet2 = ret
				ret = ret.reduceByKey(alternating.reduceMessageByKey).cache

				val rddForConvergence = ret.map(t => (t._1, Math.min(t._2.min, t._1))).cache
				control = rddForConvergence.leftOuterJoin(previousRDDForConvergence).map(t => if(t._2._2.isDefined) t._2._1 == t._2._2.get else false).cache.reduce{case(a,b) => a&&b}
				previousRDDForConvergence = rddForConvergence
				
				val timeStepSmall = System.currentTimeMillis()

				stats.printSimplificationAlternating(step + 3, ret)
				step = step + 3
				util.io.printTime(timeStepLarge, timeStepSmall, "small")
				util.printTimeStep(step+1, timeStepSmall-timeStepLarge)
				
				ret.first()
				previousRet.unpersist()
				previousRet2.unpersist()
			}

			val timeAdjustingAdditionalVertexForLoadBalancingStart = System.currentTimeMillis()
			
			val rddLabeled = ret.map(t=> (t._1, t._2.min))
			val rddLabeledInverted = rddLabeled.map(t=> (t._2, t._1))
			
			val resultJoin = rddLabeledInverted.leftOuterJoin(rddLabeled).map(t=>(t._2._1, t._2._2)).filter(t=>t._2.isDefined).map(t=>(t._1,t._2.get))
			val result = rddLabeled.leftOuterJoin(resultJoin).map(t=> if(t._2._2.isDefined) (t._1, Math.min(t._2._1,t._2._2.get)) else (t._1, t._2._1))
			
			val timeEnd = System.currentTimeMillis()
			util.io.printTime(timeAdjustingAdditionalVertexForLoadBalancingStart, timeEnd, "timeAdjustingAdditionalVertexForLoadBalancingStart")
			
			util.testEnded(
					result.filter(t => t._1%property.vertexIdMultiplier==0).groupByKey.map(t=> (t._2.min, 1)).reduceByKey{case (a,b)=> a+b}.map(t=>(t._1, t._2)),
            				step, 
            				timeBegin, 
            				timeEnd, 
            				timeSparkLoaded, 
            				timeDataLoaded, 
            				stats.reduceInputMessageNumberAccumulator.value,
            				stats.reduceInputSizeAccumulator.value)
			
		}
}
