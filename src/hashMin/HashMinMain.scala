package hashMin

import java.io.FileWriter
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CCUtil
import util.CCProperties

object HashMinMain 
{
    def emitBlue( item : ( Long, HashMinMessage ) ) : Iterable[( Long, HashMinMessage )] =
        {
            var outputList : ListBuffer[( Long, HashMinMessage )] = new ListBuffer
            
            val min = item._2.min

            val it = item._2.neigh.iterator
            
            outputList.prepend( ( item._1, new HashMinMessage( min, min, item._2.neigh) ) )

            while(it.hasNext)
            {
                val next = it.next
                
                if(next != item._1)
                {
                    outputList.prepend( ( next, new HashMinMessage( min, -1 , Set()) ) )
                }
            }
         
            outputList.toIterable
        }

    def reduceBlue( item1 : HashMinMessage, item2 : HashMinMessage ) : HashMinMessage = 
    {
        val ret = item1.neigh ++ item2.neigh
        val min = Math.min( item1.min, item2.min )
        var minBefore = item1.minBefore
        if(minBefore == -1) minBefore = item2.minBefore
        new HashMinMessage( min, minBefore , ret )
    }

    def main( args : Array[String] ) : Unit =
        {
          val timeBegin = System.currentTimeMillis()

            val property = new CCProperties("PEGASUS", args(0)).load.getImmutable
            
            val util = new CCUtil(property)
            val spark = util.getSparkContext()
            
            val timeSparkLoaded = System.currentTimeMillis()
            val file = spark.textFile( property.dataset , property.sparkPartition)

            util.io.printFileStart(property.appName)
            
//            val (parsedData, fusedData) = util.loadVertexEdgeFile(file)
            val (parsedData, fusedData) = util.loadEdgeFromFile(file)
            var ret = fusedData.map( item => ( item._1, new HashMinMessage( item._2.toSet.min, -1, item._2.toSet) ) )

            val timeDataLoaded = System.currentTimeMillis()

            var control = false;
            var step = 0
            
            val reduceInputMessageNumberAccumulator = spark.accumulator(0L)
            val reduceInputSizeAccumulator = spark.accumulator(0L)

            while ( !control ) {
                val timeStepStart = System.currentTimeMillis()

                val previous = ret
                val mapResult = ret.flatMap( item => emitBlue( item ) )
                
                if(property.printMessageStat)
                {
                	val previousMessageSize = reduceInputSizeAccumulator.value
						val previousMessageNumber = reduceInputMessageNumberAccumulator.value
						
                	mapResult.foreach(t => reduceInputSizeAccumulator += t._2.neigh.size + 2)
                	reduceInputMessageNumberAccumulator += mapResult.count
                	
                	util.printMessageStep(step + 1, reduceInputMessageNumberAccumulator.value - previousMessageNumber, reduceInputSizeAccumulator.value - previousMessageSize)
                }
                
                ret = mapResult.reduceByKey( reduceBlue ).cache

                val controlMap = ret.map(t => t._2.voteToHalt)
//                val check = controlMap.filter(t=> (!t)).count
//                util.io.printStat(check, "active")
                control = controlMap.reduce{case (a,b) => a && b}

                val timeStepBlue = System.currentTimeMillis()

                step = step + 1
                util.io.printTime( timeStepStart, timeStepBlue, "blue" )
                util.printTimeStep(step, timeStepBlue-timeStepStart)
                
                ret.checkpoint
                mapResult.unpersist(false)
                previous.unpersist(false)
                controlMap.unpersist(false)
            }

            val timeEnd = System.currentTimeMillis()
            
            util.testEnded(	ret.map(t=> (t._2.min, 1)).reduceByKey{case (a,b)=> a+b}, 
            				step, 
            				timeBegin, 
            				timeEnd, 
            				timeSparkLoaded, 
            				timeDataLoaded, 
            				reduceInputMessageNumberAccumulator.value, 
            				reduceInputSizeAccumulator.value)
        }

}