package com.deloitte.beam.transformation;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvFileFullReader {
	private static final Logger logger = LoggerFactory.getLogger(CsvFileFullReader.class);

	public static void main(String[] args) {
		logger.info("Starting main method in CsvFileFullReader");

		try {
			
			Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create()); // Creating
																											// Pipeline

			PCollection<String> vals = p.apply(TextIO.read().from("/src/main/resources/FL_insurance_sample.csv")); // reading
																													// the
																													// input
																													// .csv
																													// file
			vals.apply(ParDo.of(new RemoveHeader())) // remove header from the csvfile
					.apply(ParDo.of(new ColumnSelector())) // select the columns according to requirement
					.apply(ParDo.of(new DecimalConverter()))// Convert the Decimal Columns into Integer
					.apply(TextIO.write().to("./src/main/resources/FL_insurance_sample_output").withSuffix(".csv")
							.withNumShards(1));
			// write output to csv file

			p.run().waitUntilFinish();
		} catch (Exception e) {
			logger.info("Exception main method in CsvFileFullReader",e);
		}
		logger.info("Completed main method in CsvFileFullReader");
	}

}
