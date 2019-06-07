package com.cefet.bag.graph;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import org.javatuples.Pair;

import com.google.common.collect.Sets;

import accumulator.InBagAccumulator;
import accumulator.LevelAccumulator;
import accumulator.LevelSetAccumulator;
import accumulator.SetDifferenceAccumulator;
import classdataset.LevelBag;
import csr.Csr;
import register.InBagAccumulatorRegister;
import register.LevelAccumulatorRegister;
import register.LevelSetAccumulatorRegister;
import register.SetDifferenceAccumulatorRegister;
import structure.Bag;
import structure.Node;
import structure.Tuple;

public class App 
{
	public static void main( String[] args )
	{
		Logger.getLogger("org").setLevel(Level.ERROR);

		String path = "./data/gre_343.csv";

		Integer source = 1;

		SparkSession spark = SparkSession.
				builder().
				appName("Bag Graph").
				master("local").
				getOrCreate();

		SparkContext sc = spark.sparkContext();
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

		// Declaração dos acumuladores
		LevelSetAccumulator levelAccumulator = new LevelSetAccumulator();
		new LevelSetAccumulatorRegister(sc, levelAccumulator).register();

		InBagAccumulator inBagAccumulator = new InBagAccumulator();
		new InBagAccumulatorRegister(sc,inBagAccumulator).register();

		SetDifferenceAccumulator setDifference = new SetDifferenceAccumulator();
		new SetDifferenceAccumulatorRegister(sc,setDifference).register();

		LevelAccumulator levelAccum = new LevelAccumulator(0);
		new LevelAccumulatorRegister(sc,levelAccum).register();

		// Declaração dos Datasets
		JavaRDD<Bag> inBagRDD = 
				jsc.parallelize(Arrays.asList(new Bag()));

		JavaRDD<Bag> outBagRDD = 
				jsc.parallelize(Arrays.asList(new Bag()));

		JavaRDD<LevelBag> levelRDD = 
				jsc.parallelize(Arrays.asList
						(new LevelBag(new Node(source),levelAccum.value())));

		Dataset<Bag> inBagsDataSet = 
				spark.
				createDataset(inBagRDD.rdd(), 
						Encoders.kryo(structure.Bag.class));

		Dataset<Bag> outBagsDataSet = 
				spark.
				createDataset(outBagRDD.rdd(), 
						Encoders.kryo(structure.Bag.class));

		Dataset<LevelBag> levelDataSet = 
				spark.
				createDataset(levelRDD.rdd(), 
						Encoders.bean(classdataset.LevelBag.class));

		Dataset<Row> dataEdges = spark.
				read().
				format("csv").
				option("header", "true").
				load(path);		
				
		// Criação da instância de grafo
		GraphFrame graph = GraphFrame.fromEdges(dataEdges);		

		// Inserção do nó de origem em inBag
		JavaRDD<Bag> inBag = inBagsDataSet.toJavaRDD().map(f-> {
			f.insert(source);
			return f;
		}).cache();		

		// Criação da variável Dataset outBag
		JavaRDD<Bag> outBag = outBagsDataSet.toJavaRDD().cache();

		// Criação da variável LevelBag level, para armazenar <Node,level>
		JavaRDD<LevelBag> level = levelDataSet.toJavaRDD().cache();
		
		// Criação da estrutura de dados CSR
		Csr csr = Csr.fromEdges(sc, graph);		
			
		// Adição do nível
		levelAccum.add(1);

		Set<Integer> inBagList = new HashSet<Integer>();
		inBagList.add(source);

		while(!inBagList.isEmpty()){	
			for(Integer current_node: inBagList){
				int currentLevel = levelAccum.value();
				
				Set<Integer> neighbour= csr.getNeighbours(current_node);				
				
				JavaRDD<Set<Integer>> nodeDifference = level.map(r ->{
					List<Integer> array = r.getArrayNodes();
					Set<Integer> setNode = new HashSet<Integer>();
					for(Integer nodeIn:array) {
						setNode.add(nodeIn);
					}
					return setNode;
				}).map(f->{
					Set<Integer> setNeighbour = neighbour;
					Set<Integer> setDiff = 
							new HashSet<Integer>
					(Sets.difference(setNeighbour, f));

					return setDiff;
				}).cache();

				nodeDifference.foreach(f -> {
					setDifference.add(f);
				});

				Set<Integer> nodesInsertOutbag = setDifference.value();

				outBag = outBag.map(f->{
					for(Integer nodes:nodesInsertOutbag) {
						f.insert(nodes);	
					}
					return f;					
				}).cache();

				level = level.map(f->{
					for(Integer nodes:nodesInsertOutbag) {
						f.addLevel(new Node(nodes),currentLevel);	
					}
					return f;
				}).cache();
				
			}
			//Adição do nível
			levelAccum.add(1);

			// Reseta os nós em inBag
			inBag = inBag.map(f->{
				f.clear();
				return f;
			}).cache();

			inBag = outBag;

			inBag.foreach(f->{
				for(Integer nodes: f.write_array().getArray()) {
					inBagAccumulator.add(nodes);
				}				
			});

			inBagList = inBagAccumulator.value();

			outBag = outBag.map(f->{
				f.clear();				
				return f;
			}).cache();			
			levelAccumulator.reset();
			inBagAccumulator.reset();
			setDifference.reset();
		}

//		for(LevelBag l: level.collect()) {
//			for( Pair<Node, Integer> a:l.getSetLevel()) {
//				System.out.println("nó " + a.getValue0().getItem() + 
//						" - nível " + a.getValue1());
//			}
//		}

		level.foreach(f->{
			Map<Integer,Integer> map = f.getLevelConsolidate();
			System.out.println(map);
		});

		spark.close();
	}
}
