package com.deloitte.beam.transformation;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddingValuesByKey extends DoFn<KV<String, Iterable<Integer>>, String> {
	private static final Logger logger = LoggerFactory.getLogger(AddingValuesByKey.class);

	 @ProcessElement
	    public void processElement(ProcessContext context) {
		 logger.info("Starting processElement in AddingValuesByKey");
		 try {
			 String brand = context.element().getKey();
		        Iterable<Integer> sales = context.element().getValue();
		        context.output(brand + ": " + sales); //getting key and values and displaying it in specific format. 
		 }catch(Exception e) {
			 logger.error("Exception in AddingValuesByKey class",e);
		 }
	
	        
	        logger.info("Completed processElement in AddingValuesByKey");
	    }
}
