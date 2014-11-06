package hashToMin

import java.io.FileWriter
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CCUtil
import util.CCProperties
import util.CCProperties

object HashToMinMain 
{
    def emitBlue( item : ( Long, HashToMinMessage ) ) : Iterable[( Long, HashToMinMessage )] =
        {
            var outputList : ListBuffer[( Long, HashToMinMessage )] = new ListBuffer
            
            val min = item._2.min

            val it = item._2.cc.iterator
            
            if(min == item._1)
            {
                outputList.prepend( ( item._1, new HashToMinMessage( min, item._2.cc , item._2.cc.size) ) )
            } else
            {
                outputList.prepend( ( item._1, new HashToMinMessage( min, Set( min ) , item._2.cc.size) ) )
            }

            while(it.hasNext)
            {
                val next = it.next
                
                if(next != item._1)
                {
	                if(next == min)
	                {
	                    outputList.prepend( ( next, new HashToMinMessage( min, item._2.cc , -1) ) )
	                } else
	                {
	                	outputList.prepend( ( next, new HashToMinMessage( min, Set( min ) , -1) ) )
	                }
                }
            }
         
            outputList.toIterable
        }

    def reduceBlue( item1 : HashToMinMessage, item2 : HashToMinMessage ) : HashToMinMessage = 
    {
        val ret = item1.cc ++ item2.cc
        val min = Math.min( item1.min, item2.min )
        var size = item1.sizeBefore
        if(size == -1) size = item2.sizeBefore
        new HashToMinMessage( min, ret , size )
    }

    def main( args : Array[String] ) : Unit =
        {
           val timeBegin = System.currentTimeMillis()

            val property = new CCProperties("HASHTOMIN", args(0)).load.getImmutable
            
            val util = new CCUtil(property)
            val spark = util.getSparkContext()
            
            val timeSparkLoaded = System.currentTimeMillis()
            val file = spark.textFile( property.dataset , property.sparkPartition)

            util.io.printFileStart(property.appName)
            
//            val (parsedData, fusedData) = util.loadVertexEdgeFile(file)
            val (parsedData, fusedData) = util.loadEdgeFromFile(file)
            
            var ret = fusedData.map( item => ( item._1, new HashToMinMessage( item._2.toSet.min, item._2.toSet, -1) ) )

            val timeDataLoaded = System.currentTimeMillis()

            var control = false;
            var step = 0
            
            val reduceInputMessageNumberAccumulator = spark.accumulator(0L)
            val reduceInputSizeAccumulator = spark.accumulator(0L)

            while ( !control ) 
            {
                val timeStepStart = System.currentTimeMillis()

                val previous = ret
                val retMap = ret.flatMap( item => emitBlue( item ) )
                
                if(property.printMessageStat)
                {
                	val previousMessageSize = reduceInputSizeAccumulator.value
						val previousMessageNumber = reduceInputMessageNumberAccumulator.value
						
                	retMap.foreach(t => reduceInputSizeAccumulator += t._2.cc.size + 2)
                	reduceInputMessageNumberAccumulator += retMap.count
                	
                	util.printMessageStep(step + 1, reduceInputMessageNumberAccumulator.value - previousMessageNumber, reduceInputSizeAccumulator.value - previousMessageSize)
                }
                
                ret = retMap.reduceByKey( reduceBlue ).cache
                ret.foreach(x=>{})

                val controlMap = ret.map(t => t._2.voteToHalt)
                control = controlMap.reduce{case (a,b) => a && b}
//                try
//                {
//                	control = controlMap.reduce{case (a,b) => a && b}
//                }
//                catch
//                {
//                	case e : Exception => control = false
//                }

                val timeStepBlue = System.currentTimeMillis()

                step = step + 1
                util.io.printTime( timeStepStart, timeStepBlue, "blue" )
                util.printTimeStep(step, timeStepBlue-timeStepStart)
                
                ret.checkpoint
                previous.unpersist(false)
                retMap.unpersist(false)
                controlMap.unpersist(false)
            }

            val timeEnd = System.currentTimeMillis()
            
            util.testEnded(	ret.map(t => (t._2.min, t._2.cc.size)).reduceByKey{case (a,b) => Math.max(a, b)}, 
            				step, 
            				timeBegin, 
            				timeEnd, 
            				timeSparkLoaded, 
            				timeDataLoaded, 
            				reduceInputMessageNumberAccumulator.value, 
            				reduceInputSizeAccumulator.value)
        }

}