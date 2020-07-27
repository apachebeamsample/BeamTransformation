package com.deloitte.beam.transformation;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse.File;

//CoGroupByKey performs a relational join of two or more key/value PCollections that have the same key type

public class CoGroupbykey {
	private static final Logger logger = LoggerFactory.getLogger(CoGroupbykey.class);

	public static void main(String args[]) {
		logger.info("Starting main method in CoGroupbykey");
		try {
			Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create()); //Creating pipeline

			final List<KV<String, String>> emailsList = Arrays.asList(KV.of("amy", "amy@example.com"),
					KV.of("carl", "carl@example.com"), KV.of("julia", "julia@example.com"),
					KV.of("carl", "carl@email.com")); //Creating arraylist

			final List<KV<String, String>> phonesList = Arrays.asList(KV.of("amy", "111-222-3333"),
					KV.of("james", "222-333-4444"), KV.of("amy", "333-444-5555"), KV.of("carl", "444-555-6666")); //Creating arraylist

			PCollection<KV<String, String>> emails = p.apply("CreateEmails", Create.of(emailsList)); //Converting list into PCollection
			PCollection<KV<String, String>> phones = p.apply("CreatePhones", Create.of(phonesList));

			final TupleTag<String> emailsTag = new TupleTag<>(); //Creating intermediate  PCollection
			final TupleTag<String> phonesTag = new TupleTag<>();

			PCollection<KV<String, CoGbkResult>> results = KeyedPCollectionTuple.of(emailsTag, emails)
					.and(phonesTag, phones).apply(CoGroupByKey.create());  //Applying CoGroupBy on TupleTags
			

			results.apply(ParDo.of(new CoGroupByFunction(emailsTag, phonesTag))) //Applying transformation according to requirement
					.apply(TextIO.write().to("./src/main/output/CogroupByOutput.txt").withNumShards(1));//Writing o/p to file
			

			p.run().waitUntilFinish();
			
		}catch(Exception e) {
		logger.error("Exception in main method in CoGroupbykey ",e);	
		}
		logger.info("Completed main method in CoGroupbykey");
	}

}
