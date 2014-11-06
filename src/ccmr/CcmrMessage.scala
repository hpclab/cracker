package ccmr

import scala.collection.immutable.TreeSet

@serializable
class CcmrMessage (val cc: TreeSet[Long], val iterationNeeded : Boolean) 
{
	def voteToHalt : Boolean = !iterationNeeded
}