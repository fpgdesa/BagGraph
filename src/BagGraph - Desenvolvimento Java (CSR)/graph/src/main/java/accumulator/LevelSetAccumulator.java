package accumulator;

import java.io.Serializable;

import org.apache.spark.util.AccumulatorV2;
import java.util.HashSet;
import java.util.Set;

public  class LevelSetAccumulator extends AccumulatorV2<Integer, Set<Integer>> implements Serializable 
	{
		private static final long serialVersionUID = -3739727823287550826L;

		private Set<Integer> _valueSource = new HashSet<Integer>();

		public LevelSetAccumulator() {
		}
		
		public LevelSetAccumulator(Integer value) {
			this._valueSource.add(value);
		}

		@Override
		public void add(Integer arg0) {
			_valueSource.add(arg0);
		}

		@Override
		public AccumulatorV2<Integer, Set<Integer>> copy() {
			return this;
		}

		@Override
		public boolean isZero() {
			return _valueSource.isEmpty();
		}

		@Override
		public void merge(AccumulatorV2<Integer, Set<Integer>> arg0) {
		   this._valueSource.addAll(arg0.value());
		}

		@Override
		public void reset() {
			_valueSource = new HashSet<Integer>();
		}

		@Override
		public Set<Integer> value() {
			return _valueSource;
		}				
	}