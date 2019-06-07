package register;

import java.io.Serializable;
import java.util.Set;

import org.apache.spark.SparkContext;
import org.apache.spark.util.AccumulatorV2;

import accumulator.InBagAccumulator;

public class InBagAccumulatorRegister implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private SparkContext sc;
	private AccumulatorV2<Integer, Set<Integer>> levelAccum = 
			new  InBagAccumulator();

	public InBagAccumulatorRegister
	(SparkContext sc, AccumulatorV2<Integer, Set<Integer>> levelAcc) {
		
		this.sc = sc;
		this.levelAccum = levelAcc;
	}

	public InBagAccumulatorRegister register() {
		sc.register(levelAccum);
		return this;
	}	
}
