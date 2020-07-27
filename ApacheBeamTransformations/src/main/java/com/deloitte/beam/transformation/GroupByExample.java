package com.deloitte.beam.transformation;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//GroupByKey is a collection of key/value pairs that represents a multimap, 
//where the collection contains multiple pairs that have the same key, but different values. 
public class GroupByExample {
	private static final Logger logger = LoggerFactory.getLogger(GroupByExample.class);

	public static void main(String[] args) {
		logger.info("Starting main method in GroupByExample");
		try {
			Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

			p.apply(TextIO.read().from("/src/main/resources/groupBySample.txt"))
					.apply("ConvertToKV", MapElements.via(new ConvertingKeyValuePairs())) // Reading a comma separated
																							// file and converting it
																							// into Key Value Pairs

					.apply(GroupByKey.<String, Integer>create()) // Applying GroupBy Transformation

					.apply("AddingValuesByKey", ParDo.of(new AddingValuesByKey())) // Grouping the values by key

					.apply(TextIO.write().to("./src/main/output/groupByOutput.txt").withNumShards(1)); // Write output
																										// to a file

			p.run().waitUntilFinish();
		} catch (Exception e) {
			logger.info("Exception  in main method in GroupByExample",e);

		}
		logger.info("Completed main method in GroupByExample");


	}

}
