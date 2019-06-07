package accumulator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.util.AccumulatorV2;

public class TailHead extends 
AccumulatorV2<Row,List<Integer>> implements Serializable {

	private List<Integer> _valueSource;

	public TailHead() {
		_valueSource = new ArrayList<Integer>();
	}

	@Override
	public void add(Row arg0) {
		int node = Integer.parseInt(arg0.getAs(0));
		_valueSource.add(node);
	}

	@Override
	public AccumulatorV2<Row, List<Integer>> copy() {
		return new TailHead();
	}

	@Override
	public boolean isZero() {
		if(this._valueSource.isEmpty()) {
			return true;
		}			
		return false;
	}

	@Override
	public void merge(AccumulatorV2<Row,List<Integer>> arg0){
		this._valueSource.addAll(arg0.value());
	}

	@Override
	public void reset() {
		_valueSource = new ArrayList<Integer>() ;
	}

	@Override
	public List<Integer> value() {
		return _valueSource;
	}				
}
