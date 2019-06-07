package structure;

import java.io.Serializable;

public class Bag implements Serializable  {

	private static final long serialVersionUID = 4125270795187769171L;
	private int forest_size;
	Pennant forest[];
	private int[] array;
	private int count;


	public Bag() {
		this.forest_size = 20;
		forest = new Pennant[this.forest_size];
		for(int i = 0; i < forest_size; i++) {
			this.forest[i] = null;
		}
	}

	public Bag(final Bag bag){
		for(int i = 0; i < forest_size; i++) {
			if(bag.getForest()[i] != null) {
				getForest()[i] = bag.getForest()[i];			
			}
		}
	}

	public Pennant[] getForest() {
		return this.forest;
	}

	public int getForest_size() {
		return this.forest_size;
	}

	public int[] getArray() {
		return this.array;
	}

	public void setArray(int [] array) {
		this.array = array;
	}

	public int getCount() {
		return this.count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public void insert(int item) {
		int i = 0;
		Pennant x =  new Pennant(item);
		while(getForest()[i] != null) {
			x = getForest()[i].pmerge(x);
			getForest()[i++] = null;
		}
		getForest()[i] = x;
	}

	public final boolean empty() {
		for( int i = 0; i < getForest_size(); i++) {
			if(getForest()[i] != null) {
				return false;
			}
		}
		return true;
	}

	public void merge(Bag bag) {
		Pennant carry = null;
		int size = getForest_size();

		for(int i = 0; i < size; i++) {
			if(getForest()[i] != null) {
				getForest()[i] = getForest()[i].pmerge_FA(bag.getForest()[i], carry);
			}else if(bag.getForest()[i] != null) {
				getForest()[i] = bag.getForest()[i].pmerge_FA(getForest()[i], carry);
			}else if(carry != null){
				getForest()[i] = carry;
				carry = null;
			}else {
				getForest()[i] = null;
			}
		}
	}

	public Bag split() {
		Bag new_bag = new Bag();
		Pennant y = getForest()[0];
		getForest()[0] = null;

		for(int i = 1; i < getForest_size(); i++) {
			if(getForest()[i] != null) {
				new_bag.getForest()[i - 1] = getForest()[i].psplit();
				getForest()[i-1] = getForest()[i];
				getForest()[i] = null;
			}
		}

		if(y != null) {		
			insert(y.getRoot().getItem());
			y.remove_all(y.getRoot());
			y = null;
		}
		return new_bag;		
	}

	public void clear(){
		for(int i = 0; i < getForest_size(); i++) {
			getForest()[i] = null;
		}
	}

	public final int size(){
		int sum = 0;
		for(int i = 0; i < getForest_size(); i++) {
			if(getForest()[i] != null) {
				sum += (int)Math.pow(2,i);
			}
		}
		return sum;
	}

	public final boolean can_split() {
		return false;
	}

	public final void print() {
		int sum = 0;
		System.out.printf("***********TREES CONTAINING ITEMS:*************\n");
		for(int i = 0; i < getForest_size();i++) {
			sum = 0;
			if(getForest()[i] != null) {
				recursive_print_sum(getForest()[i].getRoot(),sum);
				System.out.printf("forest[%d] has tree of size 2^%d = %d\n", i, i, sum);
			}
		}
		System.out.printf("\n\n***********PRINTING CONTENTS:*************\n");

		for(int i = 0; i < getForest_size(); i++){
			if (getForest()[i] != null){
				System.out.printf("forest[%d] contains: \n", i);
				recursive_print(getForest()[i].getRoot());
			}
			else
				System.out.printf("forest[%d] = NULL\n", i);
		}
		System.out.printf("\n***********FINISHED*************\n\n");
	}

	public final void recursive_print(Node node) {
		if(node.getLeft() != null) {
			recursive_print(node.getLeft());			
		}

		System.out.printf("%d\n", node.getItem());

		if(node.getRight() != null) {
			recursive_print(node.getRight());
		}
	}

	public final void recursive_print_sum(Node node, int sum) {
		sum++;
		if(sum == 1) {
			if(node.getLeft() != null) {
				recursive_print_sum(node.getLeft(), sum);
			}
		}else if(node.getLeft() != null){
			recursive_print_sum(node.getLeft(), sum);
		}
		if(node.getRight() != null) {
			recursive_print_sum(node.getRight(), sum);
		}
	}

	public void remove_all() {
		for(int i = 0; i < getForest_size(); i++){
			getForest()[i].remove_all(getForest()[i].getRoot());
			getForest()[i] = null;
		}
	}

	public int[] write_array() {
		int size = this.size();
		setArray(new int[size]);
		setCount(0);

		for(int i = 0; i < getForest_size(); i++){
			if(getForest()[i] != null) {
				recursive_write_array(getForest()[i].getRoot());
			}
		}
		return array;
	}

	public void recursive_write_array( Node node) {
		if(node.getLeft() != null) {
			recursive_write_array(node.getLeft());			
		}				
		int count  = getCount();
		getArray()[getCount()] = node.getItem();
		count++;
		setCount(count);
		if(node.getRight() != null) {
			recursive_write_array(node.getRight());
		}		
	}
}
