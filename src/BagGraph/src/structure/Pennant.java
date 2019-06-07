package structure;

import java.io.Serializable;

public class Pennant  implements Serializable {

	private static final long serialVersionUID = 2467025910181784701L;
	private Node root;

	public Pennant() {
		this.root = null;
	}	
	public Pennant(int value) {
		this.root = new Node(value);
	}

	public void setRoot(Node root) {
		this.root = root;
	}

	public Node getRoot() {
		return this.root;
	}	

	public Pennant pmerge(Pennant y) {
		if(this.getRoot() == null) {
			return y;
		}else {

			this.getRoot().setRight(y.getRoot().getLeft());
			y.getRoot().setLeft(this.getRoot());
		}
		return y;
	}

	public Pennant pmerge_FA(Pennant x, Pennant y) {
		if(x == null && y == null && this.getRoot() == null) {
			return null;
		}else if(x == null && y == null) {
			return this;
		}else if(this.getRoot() == null && x == null) {
			return y;
		}else if(this.getRoot() == null && y == null) {
			return x;
		}else if(x == null) {
			y = y.pmerge(this);
			return null;
		}else if(this.getRoot() == null) {
			y = y.pmerge(x);
			return null;
		}else if (y == null) {
			y = this.pmerge(x);
			return null;
		}else {
			y = y.pmerge(x);
			return this;
		}
	}

	public Pennant psplit() {

		if(this.getRoot() != null && this.getRoot().getLeft() != null) {
			Pennant y = new Pennant();
			y.setRoot(this.getRoot().getLeft());
			this.getRoot().setLeft(y.getRoot().getRight());
			y.getRoot().setRight(null);
			return 	y;
		}
		return null;
	}

	public void remove_all(Node node) {
		if (node.getLeft() != null) {
			remove_all(node.getLeft());
		}
		if(node.getRight() != null) {
			remove_all(node.getRight());
		}
		node = null;
	}
}

