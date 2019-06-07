package classdataset;

import java.io.Serializable;

import org.apache.spark.sql.Row;

public class LevelNode implements Serializable{
	
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
