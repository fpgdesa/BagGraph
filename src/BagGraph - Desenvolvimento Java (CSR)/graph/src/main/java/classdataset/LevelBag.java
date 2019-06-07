package classdataset;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.javatuples.Pair;

import structure.Node;

public class LevelBag implements Serializable {

	private Set<Pair<Node,Integer>> setLevel = 
			new HashSet<Pair<Node,Integer>>();


	public LevelBag() {
	}

	public LevelBag(Node node, int level) {
		setLevel.add(new Pair<Node,Integer>(node, level));
	}

	public Set<Pair<Node,Integer>> getSetLevel() {
		return setLevel;
	}

	public List<Integer> getArrayNodes(){
		List<Integer> array = new ArrayList<Integer>();

		for(Pair<Node,Integer> nodes:setLevel){				
			array.add(nodes.getValue0().getItem());			
		}		
		return array;
	}

	public void addLevel(Node node, int level) {
		setLevel.add(new Pair<Node,Integer>(node, level));
	}

	public boolean containsNode(Node node) {
		for(Pair<Node,Integer> pair: setLevel) {
			if(pair.getValue0() == node) {
				return true;
			}
		}
		return false;
	}
}
