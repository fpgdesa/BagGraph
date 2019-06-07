package register;

import java.io.Serializable;

import org.apache.spark.SparkContext;
import org.apache.spark.util.AccumulatorV2;

import accumulator.LevelAccumulator;

public class LevelAccumulatorRegister implements Serializable {

	private SparkContext sc;
	private AccumulatorV2<Integer, Integer> levelAccum = 
			new  LevelAccumulator();

	public LevelAccumulatorRegister
	(SparkContext sc, AccumulatorV2<Integer, Integer> levelAcc) {
		
		this.sc = sc;
		this.levelAccum = levelAcc;
	}

	public LevelAccumulatorRegister register() {
		sc.register(levelAccum);
		return this;
	}	
}
