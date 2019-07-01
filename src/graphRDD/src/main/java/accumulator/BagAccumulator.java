package accumulator;

import java.io.Serializable;

import org.apache.spark.util.AccumulatorV2;

import structure.Bag;

public class BagAccumulator extends 
AccumulatorV2<Integer,Bag> implements Serializable {

	private static final long serialVersionUID = 1L;
	private Bag _valueSource;

	public BagAccumulator() {
		_valueSource = new Bag();
	}

	@Override
	public void add(Integer arg0) {			
		_valueSource.insert(arg0);
	}

	@Override
	public AccumulatorV2<Integer,Bag> copy() {
		return new BagAccumulator();
	}

	@Override
	public boolean isZero() {
		if(_valueSource.empty()) {
			return true;
		}			
		return false;
	}

	@Override
	public void merge(AccumulatorV2<Integer,Bag> arg0){
		this._valueSource.merge(arg0.value());
	}

	@Override
	public void reset() {
		_valueSource = new Bag() ;
	}
	@Override
	public Bag value() {
		return _valueSource;
	}			
}
