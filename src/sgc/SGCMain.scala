package sgc

import java.io.FileWriter
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CCUtil
import util.CCProperties
import util.CCProperties
import org.apache.spark.Accumulator

object SGCMain
{
    def forestInitializationStart( node : (Long, Set[Long])) : Iterable[( Long, (Set[Long], Set[Long] ))] = // id, neighbour, per cui sono min
    {
    	var outputList : ListBuffer[( Long, (Set[Long], Set[Long] ))] = new ListBuffer

    	val min = Math.min(node._2.min, node._1)

    	if(min != node._1)
        {
            outputList.prepend( ( min, (Set(), Set(node._1)) )) // dici al min che non è singleton
        }

    	outputList.prepend( ( node._1, (node._2, Set()) ))

    	outputList
    }

    def forestInitializationReduceStart(a : (Set[Long], Set[Long] ), b : (Set[Long], Set[Long] )) =
    {
        (a._1 ++ b._1, a._2 ++ b._2)
    }

    def forestInitializationEnd( node : (Long, (Set[Long], Set[Long]))) = // id, neighbour, p(v), child(v)
    {
        var outputList : ListBuffer[( Long, (Set[Long], Long, Set[Long] ))] = new ListBuffer

    	if(node._2._2.isEmpty) // se singleton
    	{
    	    val min = node._2._1.min

    	    outputList.prepend( ( node._1, (node._2._1, min, Set()) ))
    	    outputList.prepend( ( min, (Set(), -1, Set(node._1)) ))
    	} else
    	{
    	    val min = Math.min(node._2._1.min, node._1)

    	    outputList.prepend( ( node._1, (node._2._1, min, node._2._2) ))
    	}

        outputList
    }

    def forestInitializationEndReduce(a : (Set[Long], Long, Set[Long] ), b : (Set[Long], Long, Set[Long] )) =
    {
        (a._1 ++ b._1, Math.max(a._2, b._2), a._3 ++ b._3)
    }

    def starDetectionRule1(node : ( Long, (Set[Long], Long, Set[Long] ))) : Iterable[( Long, (Set[Long], Long, Long, Set[Long] ))] = // neighbor, p(v), p(p(v)),child
    {
    	var outputList : ListBuffer[( Long, (Set[Long], Long, Long, Set[Long] ))] = new ListBuffer

    	val it = node._2._3.iterator // iteratore child

        while(it.hasNext)
        {
            val next = it.next
            outputList.prepend( ( next, (Set(), -1, node._2._2, Set()) ) )
        }

    	if(node._2._2 == node._1) // se min = p(v) == p(p(v))
    	{
    	    outputList.prepend( ( node._1, (node._2._1, node._2._2, node._2._2, node._2._3) ) )
    	} else
    	{
    	    outputList.prepend( ( node._1, (node._2._1, node._2._2, -1, node._2._3) ) )
    	}

    	outputList
    }

    def starDetectionReduce1(a : (Set[Long], Long, Long, Set[Long] ), b : (Set[Long], Long, Long, Set[Long] )) =
    {
        (a._1 ++ b._1, Math.max(a._2, b._2), Math.max(a._3, b._3), a._4 ++ b._4)
    }

    def starDetectionRule1End2Start(node : ( Long, (Set[Long], Long, Long, Set[Long] ))) : Iterable[( Long, (Set[Long], Long, Boolean, Set[Long] ))] = // neighbour, min, s(v), child
    {
        var outputList : ListBuffer[( Long, (Set[Long], Long, Boolean, Set[Long] ))] = new ListBuffer

    	if(node._2._2 == node._2._3) // se p(v) == p(p(v))
    	{
    	    outputList.prepend( (node._1, (node._2._1, node._2._2, true, node._2._4)) )
    	} else
    	{
    	    outputList.prepend((node._1, (node._2._1, node._2._2, false, node._2._4)) )

    	    if(node._2._3 >= 0)
    	    {
    	    	outputList.prepend( (node._2._3, (Set(), -1, false, Set())) )
    	    	// rule 2, il nonno non può essere star
    	    }
    	}

    	outputList
    }

    def starDetectionReduce2(a : (Set[Long], Long, Boolean, Set[Long] ), b : (Set[Long], Long, Boolean, Set[Long] )) =
    {
        (a._1 ++ b._1, Math.max(a._2, b._2), a._3 && b._3, a._4 ++ b._4)
    }

    def starDetectionRule3(node : ( Long, (Set[Long], Long, Boolean, Set[Long] ))) =
    {
        var outputList : ListBuffer[( Long, (Set[Long], Long, Boolean, Set[Long] ))] = new ListBuffer

        if(!node._2._3)
        {
            val it = node._2._4.iterator // iteratore child

            while(it.hasNext)
            {
                val next = it.next
                outputList.prepend( ( next, (Set(), -1, false, Set()) ) )
            }
        }

        outputList.prepend( ( node._1, (node._2._1, node._2._2, node._2._3, node._2._4) ) )

        outputList
    }

    def starDetectionReduce3(a : (Set[Long], Long, Boolean, Set[Long]), b : (Set[Long], Long, Boolean, Set[Long] )) =
    {
        (a._1 ++ b._1, Math.max(a._2, b._2), a._3 && b._3, a._4 ++ b._4)
    }

    def conditionalStartHookingPre(node : ( Long, (Set[Long], Long, Boolean ))) =
    {
        var outputList : ListBuffer[( Long, (Long, Set[Long]) )] = new ListBuffer //min, otherMin

        val it = node._2._1.iterator // iteratore neighbour

        while(it.hasNext)
        {
            val next = it.next

            outputList.prepend( ( next, (-1, Set(node._2._2)) ) )
        }

        outputList.prepend( ( node._1, (node._2._2, Set()) ) )

        outputList
    }

    def getNotMinus(a : Long, b : Long) =
    {
        if(a == -1) b
        else if(b == -1) a
        else Math.min(a, b)
    }

    def conditionalStarHookingPreReduce(a : (Long, Set[Long]), b : (Long, Set[Long])) =
    {
        (getNotMinus(a._1, b._1), a._2++b._2)
    }

    def conditionalStarHookingPreEnd(unconditional : Boolean, node : (Long, (Long, Set[Long]))) =
    {
        if(unconditional)
        {

    	val a = node._2._2.filter(t => t != node._2._1)
    	if(a.isEmpty)
    	{
    	    (node._2._1, -1L)
    	} else

        (node._2._1, a.min)
        } else
        {
            if(node._2._2.isEmpty)
                (node._2._1, -1L)
            else
            (node._2._1, node._2._2.min)
        }
    }

    def conditionalStarHookingPreEndReduce(a : Long, b : Long) =
    {
        if(a == -1) b
        else if(b == -1) a
        else
        Math.min(a, b)
    }

    def conditionalStartHooking(node : ( Long, ((Set[Long], Long, Boolean, Set[Long]), Option[Long] ))) = // neighbout, min, star, child, (minReceivedChild
    {
        var outputList : ListBuffer[( Long, (Set[Long], Long, Set[Long]) )] = new ListBuffer //min, otherMin

        if(node._2._1._3 && node._1 == node._2._1._2 && node._2._2.isDefined  && node._2._2.get != -1 && node._2._2.get < node._2._1._2) // se star e root
        {
        	outputList.prepend((node._1, (node._2._1._1, node._2._2.get, node._2._1._4)))
        	outputList.prepend((node._2._2.get, (Set(), -1, Set(node._1))))
        } else
        {
            outputList.prepend((node._1, (node._2._1._1, node._2._1._2, node._2._1._4)))
        }

        outputList
    }

    def conditionalStartHookingReduce(a : (Set[Long], Long, Set[Long]) , b : (Set[Long], Long, Set[Long]) ) =
    {
        (a._1 ++ b._1, Math.max(a._2, b._2), a._3 ++ b._3)
    }

    def unconditionalStartHooking(node : ( Long, ((Set[Long], Long, Boolean, Set[Long]), Option[Long] ))) = // neighbout, min, star, minReceivedChild
    {
        var outputList : ListBuffer[( Long, (Set[Long], Long, Set[Long]) )] = new ListBuffer //min, otherMin

        if(node._2._1._3 && node._1 == node._2._1._2 && node._2._2.isDefined && node._2._2.get != -1) // se star e root
        {
        	outputList.prepend((node._1, (node._2._1._1, node._2._2.get, node._2._1._4)))
        	outputList.prepend((node._2._2.get, (Set(), -1, Set(node._1))))
        } else
        {
            outputList.prepend((node._1, (node._2._1._1, node._2._1._2, node._2._1._4)))
        }

        outputList
    }

    def pointerJumping(node : ( Long, ((Set[Long], Long, Set[Long])))) =
    {
        var outputList : ListBuffer[( Long, (Set[Long], Long, Long) )] = new ListBuffer

         val it = node._2._3.iterator // iteratore child

        while(it.hasNext)
        {
            val next = it.next

            outputList.prepend((next, (Set(), node._2._2, -1)))
        }

        if(node._1 == node._2._2)
        {
            outputList.prepend((node._1, (node._2._1, node._2._2, node._2._2)))
        } else
        {
            outputList.prepend((node._1, (node._2._1, -1, node._2._2)))
        }

        outputList
    }

    def pointerJumpingReduce(a : (Set[Long], Long, Long), b : (Set[Long], Long, Long)) =
    {
        (a._1 ++ b._1, Math.max(a._2, b._2), Math.max(a._3, b._3))
    }

    def rebuildChild(node : ( Long, (Set[Long], Long, Long) )) =
    {
        var outputList : ListBuffer[( Long, (Set[Long], Long, Set[Long]) )] = new ListBuffer

        outputList.prepend((node._2._2, (Set(), -1, Set(node._1))))
        outputList.prepend((node._1, (node._2._1, node._2._2, Set())))

        outputList
    }

    def rebuildChildReduce(a : (Set[Long], Long, Set[Long]), b : (Set[Long], Long, Set[Long])) =
    {
        (a._1 ++ b._1, Math.max(a._2, b._2), a._3 ++ b._3)
    }

    def iteration(	util : CCUtil,
            		graph : RDD[(Long, (Set[Long], Long, Set[Long]))],
            		printStat : Boolean,
            		reduceInputSizeAccumulator : Accumulator[Long],
            		reduceInputMessageNumberAccumulator : Accumulator[Long]) =
    {
        val timeStepStart = System.currentTimeMillis()

    	val rule3 = graph	.flatMap( item => starDetectionRule1( item ) )
                						.reduceByKey(starDetectionReduce1)
                						.flatMap( item => starDetectionRule1End2Start( item ) )
                						.reduceByKey(starDetectionReduce2)
                						.flatMap( item => starDetectionRule3( item ) )
                						.reduceByKey(starDetectionReduce3)
                						.cache
        val preProcessing = rule3.map(t => (t._1, (t._2._1, t._2._2, t._2._3)))//.filter(node => node._1 != node._2._2)
        											.flatMap( item => conditionalStartHookingPre( item ) )
        											.reduceByKey(conditionalStarHookingPreReduce)
        											.map( item => conditionalStarHookingPreEnd(false, item ) )
        											.reduceByKey(conditionalStarHookingPreEndReduce)

        val starHooking = rule3.leftOuterJoin(preProcessing).flatMap(item => conditionalStartHooking(item))
        						.reduceByKey(conditionalStartHookingReduce)
        						.cache

        val ruleAfterStarHooking = starHooking	.flatMap( item => starDetectionRule1( item ) )
                						.reduceByKey(starDetectionReduce1)
                						.flatMap( item => starDetectionRule1End2Start( item ) )
                						.reduceByKey(starDetectionReduce2)
                						.flatMap( item => starDetectionRule3( item ) )
                						.reduceByKey(starDetectionReduce3)
                						.cache

        val preProcessing2 = ruleAfterStarHooking.map(t => (t._1, (t._2._1, t._2._2, t._2._3)))//.filter(node => node._1 != node._2._2)
                											.flatMap( item => conditionalStartHookingPre( item ) )
                											.reduceByKey(conditionalStarHookingPreReduce)
                											.map( item => conditionalStarHookingPreEnd(true, item ) )
                											.reduceByKey(conditionalStarHookingPreEndReduce)

        val unconditionalStarHooking = ruleAfterStarHooking.leftOuterJoin(preProcessing2).flatMap(item => unconditionalStartHooking(item))
             										.reduceByKey(conditionalStartHookingReduce).cache

        val pointerJumpingResult = unconditionalStarHooking.flatMap(item => pointerJumping(item)).reduceByKey(pointerJumpingReduce)


        val restart = pointerJumpingResult.flatMap(rebuildChild).reduceByKey(rebuildChildReduce).cache
        val termination = pointerJumpingResult.filter(t => t._2._2 != t._2._3).count


        val timeStepEnd = System.currentTimeMillis()
        util.io.printStat(termination, "termination")
        util.io.printStat(timeStepEnd - timeStepStart, "timeIteration")


        (restart, termination)
    }

    def main( args : Array[String] ) : Unit =
        {
           val timeBegin = System.currentTimeMillis()

            val property = new CCProperties("SGC", args(0)).load.getImmutable

            val util = new CCUtil(property)
            val spark = util.getSparkContext()

            val timeSparkLoaded = System.currentTimeMillis()
            val file = spark.textFile( property.dataset , property.sparkPartition)

            util.io.printFileStart(property.appName)

            val (parsedData, fusedData) = util.loadEdgeFromFile(file)

            var ret = fusedData.map( item => ( item._1, item._2.toSet) )

            val timeDataLoaded = System.currentTimeMillis()

            var control = false;


            val reduceInputMessageNumberAccumulator = spark.accumulator(0L)
            val reduceInputSizeAccumulator = spark.accumulator(0L)

            val previous = ret
            var retMap = ret.flatMap( item => forestInitializationStart( item ) )

            retMap = retMap.reduceByKey( forestInitializationReduceStart ).cache
            retMap.count

            var forestOut = retMap.flatMap( item => forestInitializationEnd( item ) )
            						.reduceByKey(forestInitializationEndReduce)

            var (graph, termination) = iteration(util, forestOut, property.printMessageStat, reduceInputMessageNumberAccumulator, reduceInputSizeAccumulator)

            var step = 2 + 14

            while(termination != 0)
            {
            	val (graph2, termination2) = iteration(util, graph, property.printMessageStat, reduceInputMessageNumberAccumulator, reduceInputSizeAccumulator)
            	graph = graph2
            	termination = termination2
            	step = step + 14
            }

            val timeEnd = System.currentTimeMillis()

            util.testEnded(	graph.map(t => (t._2._2, 1)).reduceByKey{case (a,b) => a+b},
            				step,
            				timeBegin,
            				timeEnd,
            				timeSparkLoaded,
            				timeDataLoaded,
            				reduceInputMessageNumberAccumulator.value,
            				reduceInputSizeAccumulator.value)
        }

}
