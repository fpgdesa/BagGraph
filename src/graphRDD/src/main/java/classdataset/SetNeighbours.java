package classdataset;

import java.io.Serializable;
import java.util.Set;

public class SetNeighbours implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private Set<Integer> neighbour;
	
	public SetNeighbours(Set<Integer> neighbour) {
		this.neighbour = neighbour;
	}
	public void setNeighbour(Set<Integer> neighbour) {
		this.neighbour = neighbour;
	}	
	public Set<Integer> getNeighbour(){
		return this.neighbour;
	}
}
