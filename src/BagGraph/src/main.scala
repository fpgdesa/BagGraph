import org.apache.spark._
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j._
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.util.LongAccumulator
import org.apache.spark.util.AccumulatorV2
import structure.BagAccumulator
import scala.runtime.BoxedUnit

object main extends java.io.Serializable{

	def main(srgs:Array[String]):Unit={
			Logger.getLogger("org").setLevel(Level.ERROR)

			val sc = new SparkContext("local[*]","principal")

			val path: String = "./data/sample.txt"

			val graph = GraphLoader.edgeListFile(sc,path )

	case class  VertexProperties(color: String, precedence: Option[Int] = None, level: Option[Long] = None) 


	val source: Integer = 1

	def graphParser(node: Int,level: Long): VertexProperties = {

			if( node == source){
				VertexProperties("gray",Some(0),Some(0))  
			}else{			      
				VertexProperties("white",None,Some(level))  
			}			    
			}

			class levelSet[T](var value: Set[(T,T)]) extends AccumulatorV2[T, Set[(T,T)]] {
				def this() = this(Set.empty[(T,T)])
						override def isZero: Boolean = value.isEmpty
						override def copy(): AccumulatorV2[T, Set[(T,T)]] =  new levelSet[T](value)
						override def reset(): Unit = value = Set.empty[(T,T)]
								override def add(v: T): Unit = value = value 
								def addTuple(v: T,x:T): Unit = value = value + Tuple2(v,x)
								override def merge(other: AccumulatorV2[T, Set[(T,T)]]): Unit = value = value ++ other.value
			}


			class BagSet[T](var value: Set[T]) extends AccumulatorV2[T, Set[T]] {
				def this() = this(Set.empty[T])
						override def isZero: Boolean = value.isEmpty
						override def copy(): AccumulatorV2[T, Set[T]] = new BagSet[T](value)
						override def reset(): Unit = value = Set.empty[T]
								override def add(v: T): Unit = value = value + v
								override def merge(other: AccumulatorV2[T, Set[T]]): Unit = value = value ++ other.value
								def pick(): T = {val picked = value.head ; value -= value.head;picked}
								def size():Int = value.size
			}

			var graphParsed = graph.mapVertices((t,_) => graphParser(t.toInt, -1L))

					var inBag  = new BagAccumulator
					var outBag = new BagAccumulator

					var levelSet = new levelSet[Long]()
					var level = sc.longAccumulator("level")

					sc.register(inBag, "In Bag") 
					sc.register(outBag, "Out Bag") 
					sc.register(levelSet,"Level Set")


					inBag.add(source)
					level.add(1)

					val graphBag =  {
							while(!inBag.isZero()){
								val node_array = inBag.value().write_array()

										for(currentNode <- node_array){

											val value_level = level.value
													val set_level = levelSet.value

													val a = graphParsed.triplets.
													filter(f=> f.srcId ==currentNode).
													map(f => (f.srcId,f.dstId)).
													map{f => 
													if(!set_level.exists(p => p._1 == f._2.toLong))
														f._2

											}.map(f => if(!f.isInstanceOf[BoxedUnit]) f ).collect


													for(n <- a){
														outBag.add(n.##())
													}


											val out = outBag.value().write_array().filter(a=> a > 0 )

													val newgraph = graphParsed.vertices.
													filter(pred => out.contains(pred._1.toInt)).
													map(f => 
													if(!set_level.exists(p => p._1 == f._1.toLong)) 
														levelSet.addTuple(f._1.toLong,value_level)).collect
													println(levelSet.value)
										}
								level.add(1)
								inBag.reset()
								var i = 0
								outBag.value().getForest.foreach{element =>
								inBag.value.getForest(i) = element
								i+=1						  
								}
								outBag.reset()					
							}
			}
	}
}
