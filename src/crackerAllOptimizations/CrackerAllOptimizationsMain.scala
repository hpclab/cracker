package crackerAllOptimizations

import scala.Array.canBuildFrom
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import cracker._
import util.CCProperties
import util.CCUtil
import util.CCPropertiesImmutable
import java.io.PrintWriter
import java.io.File
import java.io.FileWriter

object CrackerAllOptimizationsMain {

  def printGraph(util: CCUtil, step: Int, description: String, g: RDD[(Long, CrackerTreeMessageIdentification)]) = {
    util.io.printToFile("graph.txt", "STEP " + step + "\t[" + description + "]\t" + g.map(t => "{" + t._1 + " " + t._2.toString + "} ").reduce { case (a, b) => a + b } + "\n")
  }

  def main(args: Array[String]): Unit = {
    val timeBegin = System.currentTimeMillis()
    /*
     * additional properties:
     * crackerUseUnionInsteadOfJoin : true | false
     * crackerCoalescePartition : true | false
     */

    val propertyLoad = new CCProperties("CRACKER_ALL", args(0)).load
    val crackerUseUnionInsteadOfJoin = propertyLoad.getBoolean("crackerUseUnionInsteadOfJoin", true)
    val crackerCoalescePartition = propertyLoad.getBoolean("crackerCoalescePartition", true)
    val crackerForceEvaluation = propertyLoad.getBoolean("crackerForceEvaluation", true)
    val crackerSkipPropagation = propertyLoad.getBoolean("crackerSkipPropagation", false)

    val (edgePruning, obliviousSeed, fcs) = getOptimizations(propertyLoad.get("optimizations", "111"))

    val property = propertyLoad.getImmutable
    val cracker = new CrackerAlgorithm(property)

    val util = new CCUtil(property)
    val spark = util.getSparkContext()
    val stats = new CrackerStats(property, util, spark)

    val timeSparkLoaded = System.currentTimeMillis()
    val file = spark.textFile(property.dataset, property.sparkPartition)

    util.io.printFileStart(property.appName)

    //            val (parsedData, fusedData) = util.loadVertexEdgeFile(file)
    val (parsedData, fusedData) = util.loadEdgeFromFile(file)

    var ret = fusedData.map(item => (item._1, new CrackerTreeMessageIdentification((item._2.toSet + item._1).min, item._2.toSet)))

    val timeDataLoaded = System.currentTimeMillis()

    var control = false;
    var step = 0

    var treeRDD: Option[RDD[(Long, CrackerTreeMessageTree)]] = Option.empty

    // if not done, CC of size 1 are not recognized
    treeRDD = Option.apply(ret.map(t => (t._1, new CrackerTreeMessageTree(-1, Set()))))

    // what did i do 3 years ago!?!?!?
    def forceLoadBalancing(step: Int): Boolean = {
      step == 0 || step == 2 || step == 8 || step == 16 || step == 32
      //				step < 10 && step % 3 == 0
    }

    while (!control) {
      // simplification step
      val timeStepStart = System.currentTimeMillis()

      stats.printSimplification(step, ret)

      ret = ret.flatMap(item => cracker.emitBlue(item, true, edgePruning))

      stats.printMessageStats(step + 1, ret)

      ret = ret.reduceByKey(cracker.reduceBlue).cache

      val active = ret.count
      //				control = active == 0
      control = active <= property.switchLocal // set the number where to switch in local mode

      val timeStepBlue = System.currentTimeMillis()
      util.printTimeStep(step + 1, timeStepBlue - timeStepStart)

      if (!control) {
        stats.printSimplification(step + 1, ret)
        // reduction step
        val check = step
        val tmp = ret.flatMap(item => cracker.emitRed(item, forceLoadBalancing(check), obliviousSeed))
        if (forceLoadBalancing(check)) {
          util.io.printStat(check, "loadBalancing triggered")
        }

        stats.printMessageStats(step + 2, tmp)

        val tmpReduced = tmp.reduceByKey(cracker.reduceRed)

        ret = tmpReduced.filter(t => t._2.first.isDefined).map(t => (t._1, t._2.first.get))
        treeRDD = cracker.mergeTree(treeRDD, tmpReduced.filter(t => t._2.second.isDefined).map(t => (t._1, t._2.second.get)), crackerUseUnionInsteadOfJoin, crackerForceEvaluation)

        val timeStepEnd = System.currentTimeMillis()
        step = step + 2
        util.io.printTimeStep(timeStepStart, timeStepBlue, timeStepEnd)
        util.printTimeStep(step, timeStepEnd - timeStepBlue)
      } else {
        step = step + 1
        util.io.printTime(timeStepStart, timeStepBlue, "blue")
      }
    }

    stats.printSimplification(step, ret)

    if (fcs) // run local
    {
      val timeLocalStart = System.currentTimeMillis()
      var retCollected = ret.collect

      control = false
      var localStep = 0

      while (!control) {
        // simpli
        val tmp = retCollected.flatMap(item => cracker.emitRed(item))

        val tmpReduced = tmp.groupBy(t => t._1).toArray.map { case (group, traversable) => (group, traversable.map(t => t._2).reduce(cracker.reduceRed)) }

        retCollected = tmpReduced.filter(t => t._2.first.isDefined).map(t => (t._1, t._2.first.get))
        treeRDD = cracker.mergeTree(spark, treeRDD, tmpReduced.filter(t => t._2.second.isDefined).map(t => (t._1, t._2.second.get)), crackerUseUnionInsteadOfJoin, crackerForceEvaluation)

        // blue step
        retCollected = retCollected.flatMap(item => cracker.emitBlue(item, false))

        retCollected = retCollected.groupBy(t => t._1).toArray.map { case (group, traversable) => (group, traversable.map(t => t._2).reduce(cracker.reduceBlue)) }

        val active = retCollected.size
        //					util.io.printStat(active, "active vertices")
        control = active == 0
        localStep += 2
      }

      val timeLocalEnd = System.currentTimeMillis()
      util.io.printStat(localStep, "localStep")
      util.io.printStat(timeLocalEnd - timeLocalStart, "localTime")
    }

    if (!crackerSkipPropagation) {

      var treeRDDPropagationTmp = treeRDD.get

      if (crackerUseUnionInsteadOfJoin && crackerCoalescePartition) {
        val timeStepStart = System.currentTimeMillis()
        treeRDDPropagationTmp = treeRDDPropagationTmp.coalesce(property.sparkPartition)
        val timeStepBlue = System.currentTimeMillis()
        util.io.printTime(timeStepStart, timeStepBlue, "coalescing")
      }

      stats.printMessageStats(step, treeRDDPropagationTmp)

      var treeRDDPropagation = treeRDDPropagationTmp.reduceByKey(cracker.reducePrepareDataForPropagation).map(t => (t._1, t._2.getMessagePropagation(t._1))).cache

      control = false
      while (!control) {
        val timeStepStart = System.currentTimeMillis()
        treeRDDPropagation = treeRDDPropagation.flatMap(item => cracker.mapPropagate(item))

        stats.printMessageStats(step + 1, treeRDDPropagation)

        treeRDDPropagation = treeRDDPropagation.reduceByKey(cracker.reducePropagate).cache
        control = treeRDDPropagation.map(t => t._2.min != -1).reduce { case (a, b) => a && b }

        step = step + 1
        val timeStepBlue = System.currentTimeMillis()
        util.io.printTime(timeStepStart, timeStepBlue, "propagation")
        util.printTimeStep(step, timeStepBlue - timeStepStart)
      }

      val timeEnd = System.currentTimeMillis()

      if (property.printLargestCC) {
        printLargestCC(spark, property, treeRDDPropagation, parsedData)
      }

      if(property.printAll) {
        treeRDDPropagation.map(t => t._1+" "+t._2.min).saveAsTextFile(property.outputFile)
      }

      util.testEnded(treeRDDPropagation.map(t => (t._2.min, 1)).reduceByKey { case (a, b) => a + b },
        step,
        timeBegin,
        timeEnd,
        timeSparkLoaded,
        timeDataLoaded,
        stats.reduceInputMessageNumberAccumulator.value,
        stats.reduceInputSizeAccumulator.value,
        getBitmaskStat(crackerUseUnionInsteadOfJoin, crackerCoalescePartition, crackerForceEvaluation),
        propertyLoad.get("optimizations", "111"))
    } else {
      val timeEnd = System.currentTimeMillis()
      val vertexNumber = fusedData.count

      util.testEnded(treeRDD.get.map(t => (1L, 1)).reduceByKey { case (a, b) => a + b },
        step,
        timeBegin,
        timeEnd,
        timeSparkLoaded,
        timeDataLoaded,
        stats.reduceInputMessageNumberAccumulator.value + cracker.getMessageNumberForPropagation(step, vertexNumber),
        stats.reduceInputSizeAccumulator.value + cracker.getMessageSizeForPropagation(step, vertexNumber),
        getBitmaskStat(crackerUseUnionInsteadOfJoin, crackerCoalescePartition, crackerForceEvaluation),
        propertyLoad.get("optimizations", "111"))
    }
  }

  def bool2int(b: Boolean) = if (b) 1 else 0

  def printLargestCC(sc: SparkContext, property: CCPropertiesImmutable, tree: RDD[(Long, CrackerTreeMessagePropagation)], edgelist: RDD[(Long, Long)]) = {
    val maxCCId = tree.map(t => (t._2.min, 1)).reduceByKey { case (a, b) => a + b }.max()(new Ordering[Tuple2[Long, Int]]() {
      override def compare(x: (Long, Int), y: (Long, Int)): Int =
        Ordering[Int].compare(x._2, y._2)
    })._1

    val maxCCVertex = tree.filter(t => t._2.min == maxCCId).map(t => t._1)

    val maxCCVertexBroadcast = sc.broadcast(maxCCVertex.collect.toSet)
    val edgelistFiltered = edgelist.filter { case (s, d) => maxCCVertexBroadcast.value.contains(d) }.collect

    val writer = new FileWriter(property.filenameLargestCC, false)

    var edge = ""
    for (edge <- edgelistFiltered) {
      writer.write(edge._1 + " " + edge._2 + "\n")
    }

    writer.close()

    //		edgelistFiltered.saveAsTextFile(property.filenameLargestCC)
  }

  def getBitmaskStat(crackerUseUnionInsteadOfJoin: Boolean,
                     crackerCoalescePartition: Boolean,
                     crackerForceEvaluation: Boolean): String = {
    bool2int(crackerUseUnionInsteadOfJoin).toString + bool2int(crackerCoalescePartition).toString + bool2int(crackerForceEvaluation).toString
  }

  def getOptimizations(data: String): (Boolean, Boolean, Boolean) = {
    data match {
      case "100" => (true, false, false)
      case "010" => (false, true, false)
      case "010" => (false, false, true)
      case _ => (true, true, true)
    }
  }

}