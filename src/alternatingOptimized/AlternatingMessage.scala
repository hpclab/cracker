package alternatingOptimized

@serializable
class AlternatingMessage (val root : Boolean) 
{
	val isMarkedAsRootNode = root
}

object AlternatingMessage
{
	val empty = new AlternatingMessage(false)
}