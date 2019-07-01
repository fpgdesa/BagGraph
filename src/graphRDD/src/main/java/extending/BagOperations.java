package extending;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;

import structure.Bag;

public class BagOperations  implements Serializable{

	private static final long serialVersionUID = -2403865582283802063L;
	private JavaRDD<Bag> rdd;

	public BagOperations(JavaRDD<Bag> rdd) {
		this.rdd = rdd.cache();		
	}

	public static BagOperations BagRdd(JavaRDD<Bag> rdd) {
		return new BagOperations(rdd);
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
	
	public void add() {
		rdd = rdd.map(f-> f.insert(9));
	}

	public void unionBag(JavaRDD<Set<Integer>> value) {

		JavaPairRDD<Bag, Set<Integer>> rddZip = rdd.zip(value).cache(); 

		rddZip.coalesce(1);
		rdd = rddZip.map(f->{
			Bag outBag = new Bag();
			Set<Integer> set = new HashSet<Integer>();

			for(Integer a: f._2()) {
				set.add(a);
			}

			for(Integer b: f._1.write_array().getArray()) {
				set.add(b);
			}

			outBag.insertSet(set);

			return outBag;
		});
		
		rdd.cache();
	}

	public void reset() {
		rdd = rdd.map(r-> {
			r.clear();			
			return r;
		});
	}

	public JavaRDD<Bag> get(){
		return rdd.cache();
	}
}
