package register;

import java.io.Serializable;
import java.util.Set;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.util.AccumulatorV2;

import structure.Tuple;

public class FirstArrayAccumulatorRegister  implements Serializable {

	private static final long serialVersionUID = 1L;
	private SparkContext sc;
	int edges = 100;
	private AccumulatorV2<Row, Set<Tuple<Integer,Integer>>> firstArray;

	public FirstArrayAccumulatorRegister
	(SparkContext sc, AccumulatorV2<Row, Set<Tuple<Integer,Integer>>> firstArray) {
		this.sc = sc;
		this.firstArray = firstArray;
	}

	public FirstArrayAccumulatorRegister register() {
		sc.register(firstArray);
		return this;
	}	
}