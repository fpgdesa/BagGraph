package register;

import java.io.Serializable;
import java.util.Set;

import org.apache.spark.SparkContext;
import org.apache.spark.util.AccumulatorV2;

import accumulator.LevelSetAccumulator;

public class LevelSetAccumulatorRegister implements Serializable {

	private static final long serialVersionUID = 6640938981401196003L;
	private SparkContext sc;
	private AccumulatorV2<Integer, Set<Integer>> levelAccum = 
			new  LevelSetAccumulator();

	public LevelSetAccumulatorRegister
	(SparkContext sc, AccumulatorV2<Integer, Set<Integer>> levelAcc) {
		
		this.sc = sc;
		this.levelAccum = levelAcc;
	}

	public LevelSetAccumulatorRegister register() {
		sc.register(levelAccum);
		return this;
	}	
}
