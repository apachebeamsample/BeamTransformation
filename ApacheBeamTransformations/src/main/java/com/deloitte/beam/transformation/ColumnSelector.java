package com.deloitte.beam.transformation;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnSelector extends DoFn<String, String> {
	private static final Logger logger = LoggerFactory.getLogger(ColumnSelector.class);
	
    @ProcessElement
    public void processElement(ProcessContext c) {
    	logger.info("Starting processElement in ColumnSelector");
    	try {
    		 String row = (String) c.element(); //reading row
    	      String[] cols = row.split(","); //Splitting row according to a separator
    	      c.output(cols[1]+","+cols[7]+","+cols[8]); //extracting the required columns
    	}catch(Exception e) {
    		logger.error("Exception in processElement method in ColumnSelector ",e);		
    	}
     
    	logger.info("Completed processElement in ColumnSelector");
    }
  }
  