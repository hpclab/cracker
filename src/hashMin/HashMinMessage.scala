package hashMin

import scala.collection.immutable.TreeSet

@serializable
class HashMinMessage (val min: Long, val minBefore : Long, val neigh : Set[Long]) 
{
	def voteToHalt : Boolean = minBefore == min
}