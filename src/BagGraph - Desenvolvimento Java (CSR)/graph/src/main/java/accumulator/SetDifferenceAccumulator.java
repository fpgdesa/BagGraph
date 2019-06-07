package accumulator;

import java.io.Serializable;

import org.apache.spark.util.AccumulatorV2;
import java.util.HashSet;
import java.util.Set;

public  class SetDifferenceAccumulator extends AccumulatorV2<Set<Integer>, Set<Integer>> implements Serializable 
	{
		private Set<Integer> _valueSource = new HashSet<Integer>();

		public SetDifferenceAccumulator() {
		}
		
		public SetDifferenceAccumulator(Set<Integer> value) {
			this._valueSource = value;
		}

		@Override
		public void add(Set<Integer> arg0) {
			_valueSource = arg0;
		}

		@Override
		public AccumulatorV2<Set<Integer>, Set<Integer>> copy() {
			return this;
		}

		@Override
		public boolean isZero() {
			return _valueSource.isEmpty();
		}

		@Override
		public void merge(AccumulatorV2<Set<Integer>, Set<Integer>> arg0) {
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