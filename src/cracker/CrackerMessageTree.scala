package cracker

@serializable
class CrackerTreeMessageTree (val parent : Long, val child : Set[Long]) extends CrackerMessageSize
{
	def getMessageSize = child.size + 1
	
	def merge(other : Option[CrackerTreeMessageTree]) : Option[CrackerTreeMessageTree] =
	{
		if(other.isDefined)
		{
			var parentNew = parent
			
			if(parentNew == -1)
			{
				parentNew = other.get.parent
			}
			
			Option.apply(new CrackerTreeMessageTree(parentNew, child ++ other.get.child))
		} else
		{
			Option.apply(CrackerTreeMessageTree.this)
		}
	}
	
	def merge(other : CrackerTreeMessageTree) : CrackerTreeMessageTree =
	{
		var parentNew = parent
		
		if(parentNew == -1)
		{
			parentNew = other.parent
		}
		
		new CrackerTreeMessageTree(parentNew, child ++ other.child)
	}
	
	def getMessagePropagation(id : Long) = 
	{
		if(parent == -1)
		{
			new CrackerTreeMessagePropagation(id, child)
		} else
		{
			new CrackerTreeMessagePropagation(-1, child)
		}
	}
}

object CrackerTreeMessageTree
{
	def empty = new CrackerTreeMessageTree(-1, Set())
}