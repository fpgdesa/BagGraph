package accumulator;

import java.io.Serializable;

import org.apache.spark.util.AccumulatorV2;

public  class LevelAccumulator extends AccumulatorV2<Integer, Integer> implements Serializable 
	{
		private int _valueSource = 0;

		public LevelAccumulator() {
		}
		
		public LevelAccumulator(Integer value) {
			this._valueSource = value;
		}

		@Override
		public void add(Integer arg0) {
			_valueSource += arg0;
		}

		@Override
		public AccumulatorV2<Integer, Integer> copy() {
			return new LevelAccumulator(value());
		}

		@Override
		public boolean isZero() {
			if(this._valueSource == 0) {
				return true;
			}
			return false;
		}

		@Override
		public void merge(AccumulatorV2<Integer, Integer> arg0) {
		   add(arg0.value()); 
		}

		@Override
		public void reset() {
			_valueSource = 0;
		}

		@Override
		public Integer value() {
			return _valueSource;
		}				
	}