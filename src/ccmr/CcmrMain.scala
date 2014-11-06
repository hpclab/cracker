package ccmr

import java.io.FileWriter
import scala.collection.immutable.TreeSet
import scala.collection.mutable.ListBuffer
import org.apache.spark.Accumulator
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CCUtil
import util.CCProperties

// not working for livejournal dataset
object CcmrMain 
{
    def emitBlue( item : ( Long, CcmrMessage ) ) : Iterable[( Long, CcmrMessage )] =
        {
            var outputList : ListBuffer[( Long, CcmrMessage )] = new ListBuffer
            
            val vSource = item._1
            val it = item._2.cc.iterator
            
            var isLocalMaxState = false
            if(it.hasNext)
            {
	            val vFirst = it.next
	            
	            if(vSource < vFirst)
	            {
	            	isLocalMaxState = true
	            	outputList.prepend((vSource, new CcmrMessage(TreeSet(vFirst), false)))
	            } 
	            
	            var vDest = vFirst
	            while(it.hasNext)
	            {
	            	vDest = it.next
	            	if(isLocalMaxState)
	            	{
	            		outputList.prepend((vSource, new CcmrMessage(TreeSet(vDest), false)))
	            	} else
	            	{
	            		outputList.prepend((vFirst, new CcmrMessage(TreeSet(vDest), false)))
	            		outputList.prepend((vDest, new CcmrMessage(TreeSet(vFirst), false)))
	            		outputList.prepend((vSource, new CcmrMessage(TreeSet(), true)))
	            	}
	            }
	            if(vSource < vDest && !isLocalMaxState)
	            {
	            	outputList.prepend((vSource, new CcmrMessage(TreeSet(vFirst), true)))
	            }
	            
            } else
            {
            	//outputList.prepend((vSource, new CcmrMessage(TreeSet(), false)))
            }
            
            
         
            outputList.toIterable 
        }

    def reduceBlue( item1 : CcmrMessage, item2 : CcmrMessage ) : CcmrMessage = 
    {
        new CcmrMessage( item1.cc ++ item2.cc, item1.iterationNeeded || item2.iterationNeeded )
    }
    
    def main( args : Array[String] ) : Unit =
        {
           val timeBegin = System.currentTimeMillis()

            val property = new CCProperties("CCMR", args(0)).load.getImmutable
            
            val util = new CCUtil(property)
            val spark = util.getSparkContext()
            
            val timeSparkLoaded = System.currentTimeMillis()
            val file = spark.textFile( property.dataset , property.sparkPartition)

            util.io.printFileStart(property.appName)
            
//            val (parsedData, fusedData) = util.loadVertexEdgeFile(file)
            val (parsedData, fusedData) = util.loadEdgeFromFile(file)
            
            def toTreeSet(data : Set[Long]) : TreeSet[Long] =
            {
            	var toReturn : TreeSet[Long] = TreeSet()
            	val it = data.iterator
            	while (it.hasNext)
            	{
            		toReturn = toReturn + it.next
            	}
            	
            	toReturn
            }

            var ret = fusedData.map( item => ( item._1, new CcmrMessage( toTreeSet(item._2.toSet), false) ) )

            val timeDataLoaded = System.currentTimeMillis()
//            // ccmr not correctly handle isolated vertices, these must be removed before starting the algorithm
//            ret = ret.filter(t => !t._2.cc.isEmpty)

            var control = false;
            var step = 0
            
            val reduceInputMessageNumberAccumulator = spark.accumulator(0L)
            val reduceInputSizeAccumulator = spark.accumulator(0L)

            while ( !control ) {
                val timeStepStart = System.currentTimeMillis()

                ret = ret.flatMap( item => emitBlue( item ) )

                if(property.printMessageStat)
                {
                	val previousMessageSize = reduceInputSizeAccumulator.value
						val previousMessageNumber = reduceInputMessageNumberAccumulator.value
                	
                	ret.foreach(t => reduceInputSizeAccumulator += t._2.cc.size + 1)
                	reduceInputMessageNumberAccumulator += ret.count
                	
                	util.printMessageStep(step + 1, reduceInputMessageNumberAccumulator.value - previousMessageNumber, reduceInputSizeAccumulator.value - previousMessageSize)
                }
                
                ret = ret.reduceByKey( reduceBlue ).cache

                val controlMap = ret.map(t => t._2.voteToHalt)
//                val test = controlMap.filter(t=>(!t)).count
//                util.io.printStat(test, "active")
                control = controlMap.reduce{case (a,b) => a && b}

                val timeStepBlue = System.currentTimeMillis()

                step = step + 1
                util.io.printTime( timeStepStart, timeStepBlue, "blue" )
                util.printTimeStep(step, timeStepBlue-timeStepStart)
            }
            
            val timeEnd = System.currentTimeMillis()
            
            
            util.testEnded(	ret.filter(t => !t._2.cc.isEmpty).map(t => (t._1, t._2.cc.size + 1)), 
            				step, 
            				timeBegin, 
            				timeEnd, 
            				timeSparkLoaded, 
            				timeDataLoaded, 
            				reduceInputMessageNumberAccumulator.value, 
            				reduceInputSizeAccumulator.value)
        }
}
