package register;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.util.AccumulatorV2;

public class TailHeadRegister  implements Serializable {

	private static final long serialVersionUID = 1L;
	private SparkContext sc;
	int edges = 100;
	private AccumulatorV2<Row, List<Integer>> tailHead;

	public TailHeadRegister
	(SparkContext sc, AccumulatorV2<Row, List<Integer>> firstArray) {
		this.sc = sc;
		this.tailHead = firstArray;
	}

	public TailHeadRegister register() {
		sc.register(tailHead);
		return this;
	}	
}