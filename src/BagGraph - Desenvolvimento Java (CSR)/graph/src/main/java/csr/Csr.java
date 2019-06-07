package csr;
import org.apache.spark.sql.functions.*;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;

import accumulator.FirstArrayAccumulator;
import accumulator.TailHead;
import register.FirstArrayAccumulatorRegister;
import register.TailHeadRegister;
import structure.Tuple;

public class Csr implements Serializable{

	private static final long serialVersionUID = 1L;
	private GraphFrame dataSet;	
	private int[] firstNbr;
	private int[] nbrArray;
	private SparkContext sc;

	public Csr(SparkContext sc, GraphFrame graph) {
		this.dataSet = graph;
		this.dataSet.cache();
		this.sc = sc;
		createCSR();
	}

	public static Csr fromEdges(SparkContext sc, GraphFrame graph) {
		return new Csr(sc, graph);		
	}

	private Dataset<Row> getVertices() {
		return dataSet.vertices();
	}

	private Dataset<Row> getEdges() {
		return dataSet.edges();
	}

	private Dataset<Row> outDegree() {
		return dataSet.outDegrees();
	}

	private int countEdges() {

		Dataset<Row> dataSetEdges= getEdges();

		dataSetEdges.createOrReplaceTempView("edges");

		Dataset<Row> countEdges = dataSetEdges.sqlContext().
				sql("SELECT COUNT(*) AS count FROM edges");

		int numberEdges = 
				Integer.parseInt(Long.toString(
						countEdges.select("count").
						toJavaRDD().collect().get(0).getAs(0)));

		return numberEdges;
	}	

	

	public void createCSR() {
		
		FirstArrayAccumulator firstArrAcc = new FirstArrayAccumulator();
		new FirstArrayAccumulatorRegister(sc, firstArrAcc).register();

		TailHead tailAcc = new TailHead();
		new TailHeadRegister(sc, tailAcc).register();

		TailHead headAcc = new TailHead();
		new TailHeadRegister(sc, headAcc).register();

		Dataset<Row> graphSrc = getEdges().select("src");
		graphSrc.foreach(f ->{
			tailAcc.add(f);
		});
		
		Dataset<Row> graphDst = getEdges().select("dst");
		graphDst.foreach(f ->{
			headAcc.add(f);
		});

		Dataset<Row> graphoutDegree = outDegree();
		graphoutDegree.foreach(f->{
			firstArrAcc.add(f);
		});
		
		int maxValue;

		if(Collections.max(tailAcc.value()) > Collections.max(headAcc.value())) {
			maxValue = Collections.max(tailAcc.value());
		}else {
			maxValue = Collections.max(headAcc.value());
		}
		
		int numberEdges = countEdges();
		int numberVertices = maxValue;

		firstNbr = new int[numberVertices + 2];	
		nbrArray = new int[numberEdges];

		Integer[] tail = new Integer[numberEdges];
		Integer[] head = new Integer[numberEdges];
		
		tail = tailAcc.value().toArray(new Integer[numberEdges]);
		
		head = headAcc.value().toArray(new Integer[numberEdges]);

		Set<Tuple<Integer,Integer>> fisrtSet= firstArrAcc.value(); 

		for(Tuple<Integer,Integer> element: fisrtSet) {			
			firstNbr[element.getValue0()] =  element.getValue1();
		}

		for(int v = 0; v < (firstNbr.length - 1); v++) {
			firstNbr[v+1] =	firstNbr[v + 1] + firstNbr[v];
		}		
		
		int[] array = firstNbr.clone();
		for (int e = 0; e < numberEdges; e++) {
			int j = array[tail[e] - 1]++;
			nbrArray[j] = head[e];
		}
		
		for (int v = numberVertices; v > 1; v--) {
			firstNbr[v] = firstNbr[v-1];
		}
		firstNbr[1] = 0;
	}
	
	public Set<Integer> getNeighbours(int node) {
		int end = firstNbr[node + 1];		
		Set<Integer> neighbour = new HashSet<Integer>();
		
		for(int j = firstNbr[node]; j < end; j++) {
			neighbour.add(nbrArray[j]);
		}		
		return neighbour;
	}

	public int[] getFirstArray() {
		return firstNbr;
	}

	public int[] getNbrArray() {
		return nbrArray;
	}
}
