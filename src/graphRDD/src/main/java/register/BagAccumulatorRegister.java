package register;

import java.io.Serializable;

import org.apache.spark.SparkContext;
import org.apache.spark.util.AccumulatorV2;

import structure.Bag;

public class BagAccumulatorRegister  implements Serializable {

	private static final long serialVersionUID = 1L;
	private SparkContext sc;
	private AccumulatorV2<Integer,Bag> value;

	public BagAccumulatorRegister
	(SparkContext sc, AccumulatorV2<Integer, Bag> value) {
		this.sc = sc;
		this.value = value;
	}

	public BagAccumulatorRegister register() {
		sc.register(value);
		return this;
	}	
}
