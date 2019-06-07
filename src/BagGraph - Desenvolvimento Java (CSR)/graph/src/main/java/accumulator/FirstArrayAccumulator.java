package accumulator;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.sql.Row;
import org.apache.spark.util.AccumulatorV2;

import structure.Tuple;

public class FirstArrayAccumulator extends 
AccumulatorV2<Row,Set<Tuple<Integer,Integer>>> implements Serializable {

	private Set<Tuple<Integer,Integer>> _valueSource;

	public FirstArrayAccumulator() {
		_valueSource = new HashSet<Tuple<Integer,Integer>>();
	}

	@Override
	public void add(Row arg0) {
		int node = Integer.parseInt(arg0.getAs(0));
		int outDegree = arg0.getAs(1);
		_valueSource.add(new Tuple<Integer,Integer>(node,outDegree));
	}

	@Override
	public AccumulatorV2<Row, Set<Tuple<Integer,Integer>>> copy() {
		return new FirstArrayAccumulator();
	}

	@Override
	public boolean isZero() {
		if(this._valueSource.isEmpty()) {
			return true;
		}			
		return false;
	}

	@Override
	public void merge(AccumulatorV2<Row,Set<Tuple<Integer,Integer>>> arg0){
		this._valueSource.addAll(arg0.value());
	}

	@Override
	public void reset() {
		_valueSource = new HashSet<Tuple<Integer,Integer>>() ;
	}

	@Override
	public Set<Tuple<Integer,Integer>> value() {
		return _valueSource;
	}				
}