package sgc

import scala.collection.immutable.TreeSet

@serializable
class HashToMinMessage (val min: Long, val cc: Set[Long], val sizeBefore : Long)
{
	def voteToHalt : Boolean = sizeBefore == cc.size
}