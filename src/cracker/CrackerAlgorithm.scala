package cracker

import org.apache.spark.SparkContext._
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import java.io.FileWriter
import util.CCPropertiesImmutable

@serializable
class CrackerAlgorithm(property : CCPropertiesImmutable) {
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

	def emitBlue(item : (Long, CrackerTreeMessageIdentification), forceLoadBalancing : Boolean, edgePruning : Boolean = true) : Iterable[(Long, CrackerTreeMessageIdentification)] =
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

				if (min < item._1 || !forceLoadBalancing || !edgePruning) {
					val it = item._2.neigh.iterator
					while (it.hasNext) {
						val next = it.next
						outputList.prepend((next, new CrackerTreeMessageIdentification(min, Set(min))))
					}
				}
			}
			
//			val printFile = new FileWriter( "check.txt", true )
//
//		printFile.write("BLUE "+item._1+ "\n" )
//		
//        printFile.close

			outputList.toIterable
		}
	
	def emitRed(item : (Long, CrackerTreeMessageIdentification)) : Iterable[(Long, CrackerTreeMessageRedPhase)] = {

		emitRed(item, false)
	}

	def emitRed(item : (Long, CrackerTreeMessageIdentification), forceLoadBalancing : Boolean, obliviousSeed : Boolean = true) : Iterable[(Long, CrackerTreeMessageRedPhase)] = {

		var outputList : ListBuffer[(Long, CrackerTreeMessageRedPhase)] = new ListBuffer

		val minset : Set[Long] = item._2.neigh
		if (minset.size > 1) {
			if(property.loadBalancing || forceLoadBalancing || obliviousSeed)
			{
				outputList.prepend((item._2.min, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageIdentification(item._2.min, Set(item._2.min)))))
			}
		    else
		    {
		        outputList.prepend((item._2.min, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageIdentification(item._2.min, minset))))
		    }	
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
		
//					val printFile = new FileWriter( "check.txt", true )
//
//		printFile.write("RED "+item._1+ "\n" )
//		
//        printFile.close

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

	def mergeTree(start : Option[RDD[(Long, CrackerTreeMessageTree)]], add : RDD[(Long, CrackerTreeMessageTree)], crackerUseUnionInsteadOfJoin : Boolean, crackerForceEvaluation : Boolean) : Option[RDD[(Long, CrackerTreeMessageTree)]] =
		{
			if (start.isDefined) {
				if(crackerUseUnionInsteadOfJoin)
				{
					Option.apply(start.get.union(add))
				} else
				{
					if(crackerForceEvaluation)
					{
						val treeUpdated = start.get.leftOuterJoin(add).map(t => (t._1, t._2._1.merge(t._2._2).get))
						val forceEvaluation = treeUpdated.count
						Option.apply(treeUpdated)
					} else
					{
						Option.apply(start.get.leftOuterJoin(add).map(t => (t._1, t._2._1.merge(t._2._2).get)))
					}
				}
			} else {
				Option.apply(add)
			}
		}
	
	def mergeTree(spark : SparkContext, start : Option[RDD[(Long, CrackerTreeMessageTree)]], add : Array[(Long, CrackerTreeMessageTree)], crackerUseUnionInsteadOfJoin : Boolean, crackerForceEvaluation : Boolean) : Option[RDD[(Long, CrackerTreeMessageTree)]] =
		{
			if (start.isDefined) {
				if(crackerUseUnionInsteadOfJoin)
				{
					Option.apply(start.get.union(spark.parallelize(add)))
				} else
				{
					if(crackerForceEvaluation)
					{
						val treeUpdated = start.get.leftOuterJoin(spark.parallelize(add)).map(t => (t._1, t._2._1.merge(t._2._2).get))
						val forceEvaluation = treeUpdated.count
						Option.apply(treeUpdated)
					} else
					{
						Option.apply(start.get.leftOuterJoin(spark.parallelize(add)).map(t => (t._1, t._2._1.merge(t._2._2).get)))
					}
				}
			} else {
				Option.apply(spark.parallelize(add))
			}
		}
	
	def mergeTree(start : Option[Array[(Long, CrackerTreeMessageTree)]], add : Array[(Long, CrackerTreeMessageTree)]) : Option[Array[(Long, CrackerTreeMessageTree)]] =
		{
			if (start.isDefined) {
				Option.apply(start.get.union(add))
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
	
	def getMessageNumberForPropagation(step : Int, vertexNumber : Long) =
	{
		val stepPropagation = (step - 1) / 2
		
		(vertexNumber * stepPropagation) + vertexNumber
	}
	
	def getMessageSizeForPropagation(step : Int, vertexNumber : Long) =
	{
		val stepPropagation = (step - 1) / 2
		
		((vertexNumber * 2) * stepPropagation) - vertexNumber
	}
}