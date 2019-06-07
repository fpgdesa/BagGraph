package structure;

import java.io.Serializable;

import org.apache.spark.util.AccumulatorV2;

	public  class BagAccumulator extends AccumulatorV2<Integer,Bag> implements Serializable 
	{
		private static final long serialVersionUID = -3739727823287550826L;

		private Bag _value = new Bag();

		public BagAccumulator() {
		}

		public BagAccumulator(Bag arg0) {
			_value = arg0;
		}

		@Override
		public void add(Integer arg0) {
			_value.insert(arg0);
		}

		@Override
		public AccumulatorV2<Integer, Bag> copy() {
			return new BagAccumulator(_value);
		}

		@Override
		public boolean isZero() {
			return _value.empty();
		}

		@Override
		public void merge(AccumulatorV2<Integer, Bag> arg0) {
			_value.merge(arg0.value());
		}

		@Override
		public void reset() {
			_value = new Bag();
		}

		@Override
		public Bag value() {
			return _value;
		}
	}

