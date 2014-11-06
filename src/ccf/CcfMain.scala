package ccf

import java.io.FileWriter
import scala.collection.immutable.TreeSet
import scala.collection.mutable.ListBuffer
import org.apache.spark.Accumulator
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CCUtil
import util.CCProperties

// TODO fail to recognize component of size = 1
object CcfMain {
	def ccfIterateMap(item : (Long, Long)) : Iterable[(Long, Long)] =
		{
			var outputList : ListBuffer[(Long, Long)] = new ListBuffer

			outputList.prepend((item._1, item._2))
			outputList.prepend((item._2, item._1))

			outputList.toIterable
		}

	def ccfIterateReduce(item : (Long, Iterable[Long])) : Iterable[(Long, Long)] =
		{
			var terminate = true
			var outputList : ListBuffer[(Long, Long)] = new ListBuffer

			var min = item._1
			val it = item._2.iterator
			var valueList : List[Long] = List()

			while (it.hasNext) {
				val next = it.next
				valueList = next :: valueList
				if (next < min) {
					min = next
				}
			}

			if (min < item._1) {
				outputList.prepend((item._1, min))
				val it2 = valueList.iterator
				while (it2.hasNext) {
					val next = it2.next
					if (min != next) {
						outputList.prepend((next, min))
						terminate = false
					}
				}
			}

			if (!terminate) {
				// ack! ugly!
				outputList.prepend((-1, min))
			}

			outputList.toIterable
		}

	def ccfDedupMap(item : (Long, Long)) : ((Long, Long), Long) =
		{
			((item._1, item._2), -1)
		}

	def ccfDedupReduce(item : ((Long, Long), Iterable[Long])) : (Long, Long) =
		{
			(item._1._1, item._1._2)
		}

	def reduceBlue(item1 : CcfMessage, item2 : CcfMessage) : CcfMessage =
		{
			new CcfMessage(item1.cc ++ item2.cc, item1.terminate || item2.terminate)
		}

	def main(args : Array[String]) : Unit =
		{
			val timeBegin = System.currentTimeMillis()

            val property = new CCProperties("CCF", args(0)).load.getImmutable
            
            val util = new CCUtil(property)
			
            val spark = util.getSparkContext()
            
            val timeSparkLoaded = System.currentTimeMillis()
            val file = spark.textFile( property.dataset , property.sparkPartition)

            util.io.printFileStart(property.appName)
            
//            val (parsedData, fusedData) = util.loadVertexEdgeFile(file)
            val (parsedData, fusedData) = util.loadEdgeFromFile(file)

			var ret = parsedData //.map( item => ( item._1, new CcfMessage( toTreeSet(item._2.toSet), false) ) )

			val timeDataLoaded = System.currentTimeMillis()

			var control = false;
			var step = 0

			val reduceInputMessageNumberAccumulator = spark.accumulator(0L)
			val reduceInputSizeAccumulator = spark.accumulator(0L)

			while (!control) {
				val timeStepStart = System.currentTimeMillis()

				var tmp = ret.flatMap(item => ccfIterateMap(item)).groupByKey

				var previousMessageSize = 0L
				var previousMessageNumber = 0L
				
				if (property.printMessageStat) {
					previousMessageSize = reduceInputSizeAccumulator.value
					previousMessageNumber = reduceInputMessageNumberAccumulator.value
					
					tmp.foreach(t => reduceInputSizeAccumulator += t._2.size + 1)
					reduceInputMessageNumberAccumulator += tmp.count
				}

				ret = tmp.flatMap(ccfIterateReduce)

				control = ret.filter(t => t._1 == -1).count == 0

				ret = ret.filter(t => t._1 != -1)

				val tmp2 = ret.map(item => ccfDedupMap(item)).groupByKey

				if (property.printMessageStat) {
					tmp2.foreach(t => reduceInputSizeAccumulator += 3)
					reduceInputMessageNumberAccumulator += tmp2.count
					
					util.printMessageStep(step + 1, reduceInputMessageNumberAccumulator.value - previousMessageNumber, reduceInputSizeAccumulator.value - previousMessageSize)
				}

				ret = tmp2.map(ccfDedupReduce)

				val timeStepBlue = System.currentTimeMillis()

				step = step + 1
				util.io.printTime(timeStepStart, timeStepBlue, "blue")
				util.printTimeStep(step, timeStepBlue-timeStepStart)
			}

			val timeEnd = System.currentTimeMillis()
			
			util.testEnded(	ret.map(t=> (t._2, 1)).reduceByKey{case (a,b)=> a+b}.map(t=>(t._1, t._2 + 1)), 
            				step, 
            				timeBegin, 
            				timeEnd, 
            				timeSparkLoaded, 
            				timeDataLoaded, 
            				reduceInputMessageNumberAccumulator.value, 
            				reduceInputSizeAccumulator.value)
			
		}
}
