package structure;

public interface BagImpl {

	Pennant[] getForest();

	int getForest_size();

	int[] getArray();

	void setArray(int [] array);

	int getCount();

	void setCount(int count);

	void insert(int item);

	boolean empty();

	void merge(Bag bag);

	Bag split();

	void clear();

	int size();

	boolean can_split();

	void print();

	void recursive_print( Node node);

	void recursive_print_sum( Node node, int sum);

	void remove_all();

	void write_array();

	void recursive_write_array(Node node);

}
