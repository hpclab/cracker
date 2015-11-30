package alternating

import scala.collection.mutable.ListBuffer

@serializable
class AlternatingAlgorithm {
	def generateInitialEdge(item : (Long, Iterable[Long])) : Iterable[(Long, Set[Long])] =
	{
		var outputList : ListBuffer[(Long, Set[Long])] = new ListBuffer
		
		val it = item._2.toSet.iterator
		while (it.hasNext) {
				val next = it.next
				outputList.prepend((item._1, Set(next)))
			}
		
		outputList.toIterable
	}
	
	def smallStarMap(item : (Long, Set[Long])) : Iterable[(Long, Set[Long])] =
		{
			var outputList : ListBuffer[(Long, Set[Long])] = new ListBuffer

			val it2 = item._2.iterator
			while (it2.hasNext) {
				val next = it2.next
				if(next <= item._1)
				{
					outputList.prepend((item._1, Set(next)))
				} else
				{
					outputList.prepend((next, Set(item._1)))
				}
			}

			outputList.toIterable
		}
	
	def smallStarReduce(item : (Long, Set[Long])) : Iterable[(Long, Set[Long])] =
		{
			var outputList : ListBuffer[(Long, Set[Long])] = new ListBuffer

			var min = Math.min( item._1, item._2.min)
			val it2 = item._2.iterator
//			var valueList : Set[Long] = Set()
//
//			while (it.hasNext) {
//				val next = it.next
//				valueList = valueList + next
//				if (next < min) {
//					min = next
//				}
//			}

//			val it2 = valueList.iterator
			while (it2.hasNext) {
				val next = it2.next
				outputList.prepend((next, Set(min)))
			}
			
			outputList.prepend((item._1, Set(min)))

			outputList.toIterable
		}
	
	def largeStarMapOptimized(item: (Long, Set[Long]), limit : Int) : Iterable[(Long, Set[Long])] =
		{
			val sizeNeighborhood = item._2.size
			var outputList : ListBuffer[(Long, Set[Long])] = new ListBuffer

//			if(info.isDefined && info.get.isMarkedAsRootNode)
//			{
//				outputList.prepend((Option(item._2, item._1), Option.empty))
//			} 
//			else 
				
			val it = item._2.iterator
			
			if(item._1 == item._2.min)
			{
				while(it.hasNext)
				{
					val next = it.next
					outputList.prepend((next, Set(item._1)))
				}
			}
			else if(sizeNeighborhood > limit && item._1 %limit==0)
			{
				while(it.hasNext)
				{
					val next = it.next
					val hash = item._1 + (next % (limit-1)) + 1
					outputList.prepend((item._1, Set(hash)))
					outputList.prepend((hash, Set(next)))
				}
				
			}
			else
			{
				while(it.hasNext)
				{
					val next = it.next
					outputList.prepend((item._1, Set(next)))
					outputList.prepend((next, Set(item._1)))
				}
				
			}
			
			outputList.toIterable
		}
	
	def reduceMessageByKey(a : Set[Long], b : Set[Long]) : Set[Long] =
	{
		a++b
	}
	
		def largeStarReduceOptimized(item: (Long, Set[Long])) : Iterable[(Long, Set[Long])] =
		{
			var outputList : ListBuffer[(Long, Set[Long])] = new ListBuffer

			var min = Math.min(item._1, item._2.min)
			val it2 = item._2.iterator
			var valueList : Set[Long] = Set()

//			while (it.hasNext) {
//				val next = it.next
//				valueList = valueList + next
//				if (next < min) {
//					min = next
//				}
//			}
//
//			val it2 = valueList.iterator
			while (it2.hasNext) {
				val next = it2.next
				if (next > item._1) {
					outputList.prepend((next, Set(min)))
				}
			}
			
			outputList.prepend((item._1, Set(min)))
				
//			outputList.prepend((Option.empty, Option(item._1, new AlternatingMessage(item._1 == min))))

			outputList.toIterable
		}
		
		def largeStarMap(item: (Long, Set[Long])) : Iterable[(Long, Set[Long])] =
		{
			val sizeNeighborhood = item._2.toSet.size
			var outputList : ListBuffer[(Long, Set[Long])] = new ListBuffer

//			if(info.isDefined && info.get.isMarkedAsRootNode)
//			{
//				outputList.prepend((Option(item._2, item._1), Option.empty))
//			} 
//			else 
				
			val it = item._2.iterator
			
			while(it.hasNext)
			{
				val next = it.next
				outputList.prepend((item._1, Set(next)))
				outputList.prepend((next, Set(item._1)))
			}
			
			outputList.toIterable
		}
	
	def largeStarReduce(item : (Long, Set[Long])) : Iterable[(Long, Set[Long])] =
		{
			var outputList : ListBuffer[(Long, Set[Long])] = new ListBuffer

			var min = item._1
			val it = item._2.iterator
			var valueList : Set[Long] = Set()

			while (it.hasNext) {
				val next = it.next
				valueList = valueList + next
				if (next < min) {
					min = next
				}
			}

			val it2 = valueList.iterator
			while (it2.hasNext) {
				val next = it2.next
				if (next > item._1) {
					outputList.prepend((next, Set(min)))
				}
			}
			
			outputList.prepend((item._1, Set(min)))

			outputList.toIterable
		}
}