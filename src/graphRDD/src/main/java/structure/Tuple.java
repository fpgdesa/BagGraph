package structure;

import java.io.Serializable;

public class Tuple<X, Y> implements Serializable { 

	private static final long serialVersionUID = 1L;

	public final X x; 
	public final Y y; 
	public Tuple(X x, Y y) { 
		this.x = x; 
		this.y = y; 
	} 
	public X getValue0() {
		return this.x;
	}
	public Y getValue1() {
		return this.y;
	}
} 