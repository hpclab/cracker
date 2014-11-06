package ccf

@serializable
class CcfMessage (val cc: Set[Int], val terminate : Boolean) 
{
	def voteToHalt : Boolean = terminate
}