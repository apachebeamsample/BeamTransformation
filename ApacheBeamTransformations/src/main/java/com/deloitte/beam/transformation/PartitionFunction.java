package com.deloitte.beam.transformation;

import org.apache.beam.sdk.transforms.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionFunction<String> implements Partition.PartitionFn<String> {

	private static final Logger logger = LoggerFactory.getLogger(PartitionFunction.class);

	public int partitionFor(String elem, int numPartitions) {
		logger.info("Starting partitionFor method in PartitionFunction");
		int partitioned = 0;
		try {
			String[] result = (String[]) elem.toString().split(",", 4);
			System.out.println(result[0]);
			System.out.println(result[1]);
			int finalresult = Integer.parseInt((java.lang.String) result[1]);
			partitioned = finalresult / 10000;
		} catch (Exception e) {
			logger.error("Exception in partitionFor method in PartitionFunction");
		}
		logger.info("Completed partitionFor method in PartitionFunction");

		return partitioned;
	}
}
