package com.deloitte.beam.transformation;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//Partition splits a single PCollection into a fixed number of smaller collections.
public class PartitionExample {
	private static final Logger logger = LoggerFactory.getLogger(PartitionExample.class);

	public static void main(String[] args) throws Exception {
		logger.info("Starting main method in PartitionExample");

		try {
			
			PipelineOptions options = PipelineOptionsFactory.create();

			Pipeline p = Pipeline.create(options);

			PCollection<String> textData = p.apply(TextIO.read().from("/src/main/resources/Partition.txt"));

			PCollectionList<String> studentsByPercentile = textData.apply(Partition.of(3, new PartitionFunction())); //Partition the i/p into 3 partitions
			PCollection<String> partition = studentsByPercentile.get(1);
			partition.apply(TextIO.write().to("./src/main/resources/partitionoutput.txt"));

			p.run().waitUntilFinish();	
		}catch(Exception e) {
			logger.info("Exception in main method in PartitionExample");

		}
		logger.info("Completed main method in PartitionExample");


		

		System.exit(0);
	}
}
