package cracker

@serializable
class CrackerTreeMessagePropagation (val min : Long, val child : Set[Long]) 
{
	def getMessageSize = child.size + 1
}