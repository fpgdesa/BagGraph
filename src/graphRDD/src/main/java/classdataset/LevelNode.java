package classdataset;

import java.io.Serializable;

import org.apache.spark.sql.Row;

public class LevelNode implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private Row level;
	
	public LevelNode(Row level) {
		this.level = level;
	}
	
	public Row getLevel() {
		return this.level;
	}

	public void setLevel(Row level) {
		this.level = level;
	}
}
