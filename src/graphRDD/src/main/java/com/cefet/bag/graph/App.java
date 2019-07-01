package com.cefet.bag.graph;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

import accumulator.InBagAccumulator;
import accumulator.LevelAccumulator;
import accumulator.LevelSetAccumulator;
import accumulator.SetDifferenceAccumulator;
import classdataset.LevelBag;
import csr.Csr;
import extending.BagOperations;
import extending.LevelOperations;
import register.InBagAccumulatorRegister;
import register.LevelAccumulatorRegister;
import register.LevelSetAccumulatorRegister;
import register.SetDifferenceAccumulatorRegister;
import structure.Bag;
import structure.Node;

public class App 
{
	public static void main( String[] args ) throws IOException
	{
		Logger.getLogger("org").setLevel(Level.ERROR);

		String path =  "./data/gre_343.csv";

		Integer source = 1;

		SparkSession spark = SparkSession.
				builder().
				appName("Bag Graph").
				master("local[*]").
				getOrCreate();

		SparkContext sc = spark.sparkContext();
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
		jsc.setCheckpointDir("./checkpoint/");

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


		levelDataSet = levelDataSet.map(new MapFunction<LevelBag,LevelBag>() {

			@Override
			public LevelBag call(LevelBag value) throws Exception {
				value.addLevel(new Node(4), 10);
				
				return value;
			}

		}, Encoders.bean(classdataset.LevelBag.class));
		
		for(LevelBag a: levelDataSet.toJavaRDD().collect()) {
			System.out.println(a.getArrayNodes());
		}
		

		Dataset<Row> dataEdges = spark.
				read().
				format("csv").
				option("header", "true").
				load(path);		

		// Criação da instância de grafo
		GraphFrame graph = GraphFrame.fromEdges(dataEdges).cache();		

		// Inserção do nó de origem em inBag
		JavaRDD<Bag> inBag = inBagsDataSet.toJavaRDD().map(f-> {
			f.insert(source);
			return f;
		}).cache();		

		BagOperations inBagTeste = BagOperations.BagRdd(inBag);

		// Criação da variável Dataset outBag
		JavaRDD<Bag> outBag = outBagsDataSet.toJavaRDD().cache();

		BagOperations outBagTeste = BagOperations.BagRdd(outBag);

		// Criação da variável LevelBag level, para armazenar <Node,level>
		JavaRDD<LevelBag> level = levelDataSet.toJavaRDD().cache();

		LevelOperations levelTeste = LevelOperations.LevelRDD(level);


		// Criação da estrutura de dados CSR
		Csr csr = Csr.fromEdges(sc, graph);		

		// Adição do nível
		levelAccum.add(1);

		Set<Integer> inBagList = new HashSet<Integer>();
		inBagList.add(source);

		int iter = 0;

		long timeInit;

		timeInit =  System.currentTimeMillis();

		int numvert = csr.getFirstArray().length; 

		int[] levelArray = new int[numvert];
		for(int v = 0; v < numvert; v++) {
			levelArray[v] = -1;
		}

		levelArray[source] = 1;

		Bag inBagc = new Bag().insert(source);

		while(!inBagc.empty()){	
			iter++;

			Integer[] inside = inBagc.write_array().getArray();
			inBagc.clear();
			Set<Integer> neigh;
			for(Integer current_node : inside){
				int currentLevel = levelAccum.value();
				neigh = csr.getNeighbours(current_node);


				for(Integer i:neigh) {
					if(levelArray[i] == -1) {
						levelArray[i] = currentLevel;
						inBagc.insert(i);
					}						
				}


				JavaRDD<Set<Integer>> nodeDifference = 
						levelTeste.getNodeDifference(current_node, csr).cache();

				nodeDifference.coalesce(1);

				outBagTeste.unionBag(nodeDifference);
				//outBagTeste.get().cache();

				levelTeste.addLevel(nodeDifference, currentLevel);
				//levelTeste.getLevel().cache();

			}

			if(iter==20) {
				levelTeste.getLevel().cache();
				levelTeste.checkpoint();				
				levelTeste.getLevel().take(1);
				//inBagTeste.get().checkpoint();
				iter = 0;
			}


			levelAccum.add(1);

			//inBagTeste = outBagTeste;
			//	inBagTeste.get().checkpoint();

			//			inBagTeste.get().map(f->{
			//				for(Integer nodes: f.write_array().getArray()) {
			//					inBagAccumulator.add(nodes);
			//				}	
			//				return f;
			//			}).count();

			//inBagList = inBagAccumulator.value();
			//inBagAccumulator.reset();



			outBagTeste.reset();
		}



		long timeEnd = System.currentTimeMillis();
		System.out.println(timeEnd - timeInit);

		for( LevelBag a:levelTeste.getLevel().collect()) {
			System.out.println(a.getLevelConsolidate());
		}

		try {
			System.in.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		spark.close();
	}
}
