package com.deloitte.beam.transformation;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoGroupByFunction extends DoFn<KV<String, CoGbkResult>, String> {

	private static final Logger logger = LoggerFactory.getLogger(CoGroupByFunction.class);
	private TupleTag<String> emailsTag;
	private TupleTag<String> phonesTag;

	public CoGroupByFunction(TupleTag<String> emailsTag, TupleTag<String> phonesTag) {
		this.emailsTag = emailsTag;
		this.phonesTag = phonesTag;
	}

	@ProcessElement
	public void processElement(ProcessContext c) {
		logger.info("Starting processElement in CoGroupByFunction");
		try
		{
			KV<String, CoGbkResult> e = c.element();
			String key = e.getKey();
			CoGbkResult result = e.getValue();
			Iterable<String> allStrings = result.getAll(emailsTag); //get all the values from emailsTag PCollection
			Iterable<String> allStrings2 = result.getAll(phonesTag); //get all the values from phonesTag PCollection
			c.output(key + ";" + allStrings + " ; " + allStrings2);	
		}catch(Exception e) {
			logger.error("Exception in CoGroupByFunction class",e);
		}
		
		logger.info("Completed processElement in CoGroupByFunction");

	}

}
