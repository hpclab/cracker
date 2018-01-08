package util

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class CCUtil(property: CCPropertiesImmutable) extends Serializable {
  val io = new CCUtilIO(property)
  var vertexNumber = 0L

  def getSparkContext(): SparkContext = {
    val conf = new SparkConf()
      .setMaster(property.sparkMaster)
      .setAppName(property.appName)
      .set("spark.executor.memory", property.sparkExecutorMemory)
      .set("spark.storage.blockManagerSlaveTimeoutMs", property.sparkBlockManagerSlaveTimeoutMs)
      .setJars(Array(property.jarPath))

    if (property.sparkCoresMax > 0) {
      conf.set("spark.cores.max", property.sparkCoresMax.toString)
      val executorCore = property.sparkCoresMax / property.sparkExecutorInstances
      conf.set("spark.executor.cores", executorCore.toString)
    }
    if (property.sparkExecutorInstances > 0) {
      conf.set("spark.executor.instances", property.sparkExecutorInstances.toString)
    }

    val spark = new SparkContext(conf)

    spark.setCheckpointDir(".")

    spark
  }

  // return edgelist and edge associated to each vertex
  def loadEdgeFromFile(data: RDD[String]): (RDD[(Long, Long)], RDD[(Long, Iterable[Long])]) = {
    val toReturnEdgeList = data.flatMap(line => {
      val splitted = line.split(property.separator)
      if (splitted.size >= 1) {
        try {
          Array((splitted(0).toLong, splitted(1).toLong), (splitted(1).toLong, splitted(0).toLong))
        } catch {
          case e: Exception => Array[(Long, Long)]()
        }
      } else {
        Array[(Long, Long)]()
      }
    })

    val toReturnVertex = toReturnEdgeList.distinct.groupByKey

    if (property.printMessageStat) {
      val edgeNumber = toReturnEdgeList.count / 2
      vertexNumber = toReturnVertex.count

      io.printStat(edgeNumber, "edgeNumber")
      io.printStat(vertexNumber, "vertexNumber")
    }

    (toReturnEdgeList, toReturnVertex)
  }

  // load from a file in the format of
  // vertexID, arcID
  def loadVertexEdgeFile(data: RDD[String]): (RDD[(Long, Long)], RDD[(Long, Iterable[Long])]) = {
    def mapToEdgeList(item: (String, Iterable[Long])): Iterable[(Long, Long)] = {
      var outputList: ListBuffer[(Long, Long)] = new ListBuffer

      val it = item._2.iterator

      while (it.hasNext) {
        val next = it.next
        val it2 = item._2.iterator

        while (it2.hasNext) {
          val next2 = it2.next

          if (next != next2) {
            outputList.prepend((next, next2))
          }
        }
      }

      outputList.toIterable
    }

    val toReturnEdgeList = data.flatMap(line => {
      val splitted = line.split(",")
      if (splitted.size >= 1) {
        try {
          Array((splitted(1), splitted(0).toLong))
        } catch {
          case e: Exception => Array[(String, Long)]()
        }
      } else {
        Array[(String, Long)]()
      }
    })

    val edgeList = toReturnEdgeList.groupByKey.flatMap(mapToEdgeList)

    //			io.printEdgelist(edgeList)

    val toReturnVertex = edgeList.groupByKey

    if (property.printMessageStat) {
      val edgeNumber = toReturnEdgeList.count
      val vertexNumber = toReturnVertex.count

      io.printStat(edgeNumber, "edgeNumber")
      io.printStat(vertexNumber, "vertexNumber")
    }

    (edgeList, toReturnVertex)
  }

  def getCCNumber(rdd: RDD[(Long, Int)]) = {
    rdd.count
  }

  def getCCNumberNoIsolatedVertices(rdd: RDD[(Long, Int)]) = {
    rdd.filter(t => t._2 != 1).count
  }

  def getCCMaxSize(rdd: RDD[(Long, Int)]) = {
    rdd.map(t => t._2).max
  }

  def printSimplification(step: Int, activeVertices: Long) = {
    io.printSimplification(step, activeVertices, vertexNumber)
  }

  def printSimplification(step: Int, activeVertices: Long, activeEdges: Double, degreeMax: Int) = {
    io.printSimplification(step, activeVertices, vertexNumber, activeEdges, degreeMax)
  }

  def printTimeStep(step: Int, time: Long) = {
    if (!property.printMessageStat)
      io.printTimeStep(step, time)
  }

  def printMessageStep(step: Int, messageNumber: Long, messageSize: Long) = {
    io.printMessageStep(step, messageNumber, messageSize)
  }

  def testEnded(rdd: RDD[(Long, Int)], step: Int, timeBegin: Long, timeEnd: Long, timeSparkLoaded: Long, timeDataLoaded: Long, reduceInputMessageNumber: Long, reduceInputSize: Long, bitmask: String = "", optimization: String = "") = {
    io.printTime(timeBegin, timeEnd, "all")
    io.printTime(timeSparkLoaded, timeEnd, "allComputationAndLoadingGraph")
    io.printTime(timeDataLoaded, timeEnd, "allComputation")
    io.printStep(step)
    io.printStat(reduceInputMessageNumber, "reduceInputMessageNumber")
    io.printStat(reduceInputSize, "reduceInputSize")
    io.printFileEnd(property.appName)

    io.printAllStat(property.algorithmName,
      property.dataset,
      property.sparkPartition,
      step,
      (timeEnd - timeBegin),
      (timeEnd - timeSparkLoaded),
      (timeEnd - timeDataLoaded),
      reduceInputMessageNumber,
      reduceInputSize,
      getCCNumber(rdd),
      getCCNumberNoIsolatedVertices(rdd),
      getCCMaxSize(rdd),
      property.customColumnValue)

    if (property.printCCDistribution)
      io.printCCDistribution(rdd)
  }

  /*def testEnded(ccNumber : Long, ccNumberNoIsolatedVertices : Long, step : Int, timeBegin : Long, timeEnd : Long, timeSparkLoaded : Long, timeDataLoaded : Long, reduceInputMessageNumber : Long, reduceInputSize : Long)  =
  {
    io.printTime( timeBegin, timeEnd, "all" )
        io.printTime( timeSparkLoaded, timeEnd, "allComputationAndLoadingGraph" )
        io.printTime( timeDataLoaded, timeEnd, "allComputation" )
        io.printStep( step )
        io.printStat(reduceInputMessageNumber, "reduceInputMessageNumber")
        io.printStat(reduceInputSize, "reduceInputSize")
        io.printFileEnd(property.appName)

        io.printAllStat(	property.algorithmName,
                  property.dataset,
                property.sparkPartition,
                step,
                (timeEnd - timeBegin),
                (timeEnd - timeSparkLoaded) ,
                (timeEnd - timeDataLoaded),
                reduceInputMessageNumber,
                reduceInputSize,
                ccNumber,
                ccNumberNoIsolatedVertices,
                0,
                property.customColumnValue)
  }*/
}
