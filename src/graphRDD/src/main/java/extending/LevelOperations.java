package extending;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;

import com.google.common.collect.Sets;

import classdataset.LevelBag;
import csr.Csr;
import structure.Node;

public class LevelOperations implements Serializable {

	private static final long serialVersionUID = 1L;
	private JavaRDD<LevelBag> rdd;

	public LevelOperations(JavaRDD<LevelBag> rdd) {
		this.rdd = rdd.cache();		
	}

	public static LevelOperations LevelRDD(JavaRDD<LevelBag> rdd) {
		return new LevelOperations(rdd);
	}

	public void addLevel(JavaRDD<Set<Integer>> node, int currentLevel) {

		JavaPairRDD<LevelBag, Set<Integer>> rddZip = rdd.zip(node).cache();
		
		rddZip.coalesce(2);
		
		rdd = rddZip.map(f->{
			for(Integer a:f._2) {
				f._1().addLevel(new Node(a),currentLevel);
			}			
			return f._1;
		}).cache();		
	}	

	public JavaRDD<Set<Integer>> getNodeDifference(int currentNode,Csr csr){

		Set<Integer> neighbour = csr.getNeighbours(currentNode);

		JavaRDD<Set<Integer>> nodeDiference = rdd.map(f->{
			Set<Integer> setDiff = 
					new HashSet<Integer>
			(Sets.difference(neighbour, f.getArrayNodes()));		
			return setDiff;
		}).cache();

		return nodeDiference;
	}
	
	public void cache() {
		rdd.cache();
	}
	
	public void persist() {
		rdd.persist(StorageLevel.MEMORY_ONLY());
	}
	
	public void checkpoint() {
		rdd.checkpoint();
	}
	
	public JavaRDD<LevelBag> getLevel(){
		return rdd.cache();
	}
}
