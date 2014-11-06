package cracker

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer
import java.io.FileWriter
import org.apache.spark.rdd.RDD
import util.CCUtil
import util.CCUtil
import util.CCUtil
import util.CCProperties

object CrackerTreeMain {
	def mapPropagate(item : (Long, CrackerTreeMessagePropagation)) : Iterable[(Long, CrackerTreeMessagePropagation)] =
		{
			var outputList : ListBuffer[(Long, CrackerTreeMessagePropagation)] = new ListBuffer
			if (item._2.min != -1) {
				outputList.prepend((item._1, new CrackerTreeMessagePropagation(item._2.min, Set())))
				val it = item._2.child.iterator
				while (it.hasNext) {
					val next = it.next
					outputList.prepend((next, new CrackerTreeMessagePropagation(item._2.min, Set())))
				}
			} else {
				outputList.prepend(item)
			}
			outputList
		}

	def reducePropagate(item1 : CrackerTreeMessagePropagation, item2 : CrackerTreeMessagePropagation) : CrackerTreeMessagePropagation =
		{
			var minEnd = item1.min
			if (minEnd == -1) minEnd = item2.min

			new CrackerTreeMessagePropagation(minEnd, item1.child ++ item2.child)
		}

	def emitBlue(item : (Long, CrackerTreeMessageIdentification)) : Iterable[(Long, CrackerTreeMessageIdentification)] =
		{
			var outputList : ListBuffer[(Long, CrackerTreeMessageIdentification)] = new ListBuffer
			if (item._2.min == item._1 && (item._2.neigh.isEmpty || (item._2.neigh.size == 1 && item._2.neigh.contains(item._1)))) {
				//                outputList.prepend( ( item._1, new CrackerTreeMessage( item._2.min, Set()) ) )
			} else {

				val min = item._2.min

				if (item._2.neigh.isEmpty) {
					outputList.prepend((item._1, new CrackerTreeMessageIdentification(min, Set())))
				} else {
					outputList.prepend((item._1, new CrackerTreeMessageIdentification(min, Set(min))))
				}

				if (min < item._1) {
					val it = item._2.neigh.iterator
					while (it.hasNext) {
						val next = it.next
						outputList.prepend((next, new CrackerTreeMessageIdentification(min, Set(min))))
					}
				}
			}

			outputList.toIterable
		}

	def emitRed(item : (Long, CrackerTreeMessageIdentification)) : Iterable[(Long, CrackerTreeMessageRedPhase)] = {

		var outputList : ListBuffer[(Long, CrackerTreeMessageRedPhase)] = new ListBuffer

		val minset : Set[Long] = item._2.neigh
		if (minset.size > 1) {
			outputList.prepend((item._2.min, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageIdentification(item._2.min, minset))))
			var it = minset.iterator
			while (it.hasNext) {
				val value : Long = it.next
				if (value != item._2.min)
					outputList.prepend((value, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageIdentification(item._2.min, Set(item._2.min)))))
			}
		} else if (minset.size == 1 && minset.contains(item._1)) {
			outputList.prepend((item._1, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageIdentification(item._1, Set()))))
		}

		if (!item._2.neigh.contains(item._1)) {
			outputList.prepend((item._2.min, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageTree(-1, Set(item._1)))))
			outputList.prepend((item._1, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageTree(item._2.min, Set()))))
		}

		outputList.toIterable
	}

	def reduceBlue(item1 : CrackerTreeMessageIdentification, item2 : CrackerTreeMessageIdentification) : CrackerTreeMessageIdentification =
		{
			val ret = item1.neigh ++ item2.neigh
			val min = Math.min(item1.min, item2.min)

			new CrackerTreeMessageIdentification(min, ret)
		}

	def mergeMessageIdentification(first : Option[CrackerTreeMessageIdentification], second : Option[CrackerTreeMessageIdentification]) : Option[CrackerTreeMessageIdentification] =
		{
			if (first.isDefined) {
				first.get.merge(second)
			} else {
				second
			}
		}

	def mergeMessageTree(first : Option[CrackerTreeMessageTree], second : Option[CrackerTreeMessageTree]) : Option[CrackerTreeMessageTree] =
		{
			if (first.isDefined) {
				first.get.merge(second)
			} else {
				second
			}
		}

	def reduceRed(item1 : CrackerTreeMessageRedPhase, item2 : CrackerTreeMessageRedPhase) : CrackerTreeMessageRedPhase =
		{
			new CrackerTreeMessageRedPhase(mergeMessageIdentification(item1.first, item2.first), mergeMessageTree(item1.second, item2.second))
		}

	def mergeTree(start : Option[RDD[(Long, CrackerTreeMessageTree)]], add : RDD[(Long, CrackerTreeMessageTree)], crackerUseUnionInsteadOfJoin : Boolean) : Option[RDD[(Long, CrackerTreeMessageTree)]] =
		{
			if (start.isDefined) {
				if(crackerUseUnionInsteadOfJoin)
				{
					Option.apply(start.get.union(add))
				} else
				{
					Option.apply(start.get.leftOuterJoin(add).map(t => (t._1, t._2._1.merge(t._2._2).get)))
				}
			} else {
				Option.apply(add)
			}
		}

	def reducePrepareDataForPropagation(a : CrackerTreeMessageTree, b : CrackerTreeMessageTree) : CrackerTreeMessageTree =
		{
			var parent = a.parent
			if (parent == -1) parent = b.parent

			new CrackerTreeMessageTree(parent, a.child ++ b.child)
		}
	
	def printGraph(util : CCUtil, step : Int, description : String,  g : RDD[(Long, CrackerTreeMessageIdentification)]) =
	{
		util.io.printToFile("graph.txt", "STEP "+step+"\t["+description+"]\t"+ g.map(t => "{"+t._1 +" "+ t._2.toString+"} ").reduce{case (a,b) => a+b}+"\n")
	}

	def main(args : Array[String]) : Unit =
		{
			val timeBegin = System.currentTimeMillis()

			/*
			 * additional properties:
			 * crackerUseUnionInsteadOfJoin : true | false
			 * crackerCoalescePartition : true | false
			 */
			
			val propertyLoad = new CCProperties("CRACKER_TREE_SPLIT", args(0)).load
			val crackerUseUnionInsteadOfJoin = propertyLoad.getBoolean("crackerUseUnionInsteadOfJoin", true)
			val crackerCoalescePartition = propertyLoad.getBoolean("crackerCoalescePartition", true)
			
			val property = propertyLoad.getImmutable

			val util = new CCUtil(property)
			val spark = util.getSparkContext()

			val timeSparkLoaded = System.currentTimeMillis()
			val file = spark.textFile(property.dataset, property.sparkPartition)

			util.io.printFileStart(property.appName)

			//            val (parsedData, fusedData) = util.loadVertexEdgeFile(file)
			val (parsedData, fusedData) = util.loadEdgeFromFile(file)

			var ret = fusedData.map(item => (item._1, new CrackerTreeMessageIdentification((item._2.toSet + item._1).min, item._2.toSet)))

			val timeDataLoaded = System.currentTimeMillis()

			var control = false;
			var step = 0
			var stepToPrint = 1

			val reduceInputMessageNumberAccumulator = spark.accumulator(0L)
			val reduceInputSizeAccumulator = spark.accumulator(0L)

			var treeRDD : Option[RDD[(Long, CrackerTreeMessageTree)]] = Option.empty
			
			// if not done, CC of size 1 are not recognized
			treeRDD = Option.apply(ret.map(t => (t._1, new CrackerTreeMessageTree(-1, Set()))))

			while (!control) {
				val timeStepStart = System.currentTimeMillis()

				if (property.printMessageStat) {
					util.printSimplification((step / 2) + 1, ret.count)
				}
				
				if(property.printAll)
				{
					printGraph(util, step, "INPUT_BLUE", ret)
				}
				
				ret = ret.flatMap(item => emitBlue(item))

				if (property.printMessageStat) {
					val previousMessageSize = reduceInputSizeAccumulator.value
					val previousMessageNumber = reduceInputMessageNumberAccumulator.value
					
					ret.foreach(t => reduceInputSizeAccumulator += t._2.getMessageSize)
					reduceInputMessageNumberAccumulator += ret.count
					
					util.printMessageStep(step + 1, reduceInputMessageNumberAccumulator.value - previousMessageNumber, reduceInputSizeAccumulator.value - previousMessageSize)
				}

				ret = ret.reduceByKey(reduceBlue).cache

				val active = ret.count
				control = active == 0

				val timeStepBlue = System.currentTimeMillis()

				if (!control) {
					
					if(property.printAll)
					{
						printGraph(util, step, "INPUT_RED", ret)
					}
					
					val tmp = ret.flatMap(item => emitRed(item))

					if (property.printMessageStat) {
						val previousMessageSize = reduceInputSizeAccumulator.value
						val previousMessageNumber = reduceInputMessageNumberAccumulator.value
						
						tmp.foreach(t => reduceInputSizeAccumulator += t._2.getMessageSize)
						reduceInputMessageNumberAccumulator += tmp.count
						
						util.printMessageStep(step + 2, reduceInputMessageNumberAccumulator.value - previousMessageNumber, reduceInputSizeAccumulator.value - previousMessageSize)
					}

					val tmpReduced = tmp.reduceByKey(reduceRed)

					ret = tmpReduced.filter(t => t._2.first.isDefined).map(t => (t._1, t._2.first.get))
					treeRDD = mergeTree(treeRDD, tmpReduced.filter(t => t._2.second.isDefined).map(t => (t._1, t._2.second.get)), crackerUseUnionInsteadOfJoin)

					val timeStepEnd = System.currentTimeMillis()
					util.io.printTimeStep(timeStepStart, timeStepBlue, timeStepEnd)
					util.printTimeStep(stepToPrint, timeStepEnd-timeStepStart)
					stepToPrint = stepToPrint + 1
					step = step + 2
				} else {
					step = step + 1
					util.io.printTime(timeStepStart, timeStepBlue, "blue")
					util.printTimeStep(stepToPrint, System.currentTimeMillis()-timeStepStart)
					stepToPrint = stepToPrint + 1
				}
			}
			
			if (property.printMessageStat) {
					util.printSimplification((step + 1) / 2, ret.count)
				}

			var treeRDDPropagationTmp = treeRDD.get
			
			if(crackerUseUnionInsteadOfJoin && crackerCoalescePartition)
			{
			    val timeStepStart = System.currentTimeMillis()
				treeRDDPropagationTmp = treeRDDPropagationTmp.coalesce(property.sparkPartition)
				val timeStepBlue = System.currentTimeMillis()
				util.io.printTime(timeStepStart, timeStepBlue, "coalescing")
			}

			if (property.printMessageStat) {
				treeRDDPropagationTmp.foreach(t => reduceInputSizeAccumulator += t._2.getMessageSize)
				reduceInputMessageNumberAccumulator += ret.count
			}

			var treeRDDPropagation = treeRDDPropagationTmp.reduceByKey(reducePrepareDataForPropagation).map(t => (t._1, t._2.getMessagePropagation(t._1))).cache

			control = false
			while (!control) {
				val timeStepStart = System.currentTimeMillis()
				treeRDDPropagation = treeRDDPropagation.flatMap(item => mapPropagate(item))
				
				if (property.printMessageStat) {
					val previousMessageSize = reduceInputSizeAccumulator.value
					val previousMessageNumber = reduceInputMessageNumberAccumulator.value
					
					treeRDDPropagation.foreach(t => reduceInputSizeAccumulator += t._2.getMessageSize)
					reduceInputMessageNumberAccumulator += treeRDDPropagation.count
					
					util.printMessageStep(step + 1, reduceInputMessageNumberAccumulator.value - previousMessageNumber, reduceInputSizeAccumulator.value - previousMessageSize)
				}
				treeRDDPropagation = treeRDDPropagation.reduceByKey(reducePropagate).cache
				control = treeRDDPropagation.map(t => t._2.min != -1).reduce { case (a, b) => a && b }

				step = step + 1
				val timeStepBlue = System.currentTimeMillis()
				util.io.printTime(timeStepStart, timeStepBlue, "propagation")
				util.printTimeStep(stepToPrint, timeStepBlue-timeStepStart)
				stepToPrint = stepToPrint + 1
			}

			val timeEnd = System.currentTimeMillis()

			util.testEnded(treeRDDPropagation.map(t => (t._2.min, 1)).reduceByKey { case (a, b) => a + b },
				step,
				timeBegin,
				timeEnd,
				timeSparkLoaded,
				timeDataLoaded,
				reduceInputMessageNumberAccumulator.value,
				reduceInputSizeAccumulator.value)
		}

}