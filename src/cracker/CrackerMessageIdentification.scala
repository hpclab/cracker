package cracker

@serializable
class CrackerTreeMessageIdentification (val min: Long, val neigh: Set[Long]) extends CrackerMessageSize
{
	def voteToHalt = neigh.isEmpty
	
	def getMessageSize = neigh.size + 1
	
	def merge(other : Option[CrackerTreeMessageIdentification]) : Option[CrackerTreeMessageIdentification] =
	{
		if(other.isDefined)
		{
			Option.apply(new CrackerTreeMessageIdentification(Math.min(min, other.get.min), neigh ++ other.get.neigh))
		} else
		{
			Option.apply(CrackerTreeMessageIdentification.this)
		}
	}
	
	override def toString = neigh.toString
}

object CrackerTreeMessageIdentification
{
	def empty = new CrackerTreeMessageIdentification(-1, Set())
}