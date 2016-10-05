package cracker

@serializable
class CrackerTreeMessagePropagation (val min : Long, val child : Set[Long]) extends CrackerMessageSize
{
	def getMessageSize = child.size + 1
}