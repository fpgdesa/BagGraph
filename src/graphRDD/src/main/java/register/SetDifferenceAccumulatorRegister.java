package register;

import java.io.Serializable;
import java.util.Set;

import org.apache.spark.SparkContext;
import org.apache.spark.util.AccumulatorV2;

import accumulator.SetDifferenceAccumulator;

public class SetDifferenceAccumulatorRegister implements Serializable {

	private SparkContext sc;
	private AccumulatorV2<Set<Integer>, Set<Integer>> levelAccum =  
			new  SetDifferenceAccumulator();

	public SetDifferenceAccumulatorRegister
	(SparkContext sc, AccumulatorV2<Set<Integer>, Set<Integer>> levelAcc) {
		
		this.sc = sc;
		this.levelAccum = levelAcc;
	}

	public SetDifferenceAccumulatorRegister register() {
		sc.register(levelAccum);
		return this;
	}	
}
