package structure;

import java.io.Serializable;

public class Node  implements Serializable  {

	private static final long serialVersionUID = -6508229800097576682L;
	private Node left;
	private Node right;
	private int item;

	public Node() {
		left = null;
		right = null;
		item = 0;
	}
	public Node(int value) {
		left = null;
		right = null;
		item = value;
	}

	public Node getLeft() {
		return left;
	}
	public void setLeft(Node left) {
		this.left = left;
	}
	public Node getRight() {
		return right;
	}
	public void setRight(Node right) {
		this.right = right;
	}
	public int getItem() {
		return this.item;
	}
	public void setItem(int item) {
		this.item = item;
	}
}