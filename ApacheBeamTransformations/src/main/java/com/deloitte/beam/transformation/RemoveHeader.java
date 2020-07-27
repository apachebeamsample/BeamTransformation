package com.deloitte.beam.transformation;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoveHeader extends DoFn<String, String> {
	private static final Logger logger = LoggerFactory.getLogger(RemoveHeader.class);

	String headerFilter;

	public void RemoveHeader(String headerFilter) {
		this.headerFilter = headerFilter;
	}

	@ProcessElement
	public void processElement(ProcessContext c) {
		logger.info("Starting processElement method in RemoveHeader");
		try {
			String row = (String) c.element(); // Filter out elements that match the header if
			if (!row.equals(this.headerFilter)) {
				c.output(row);
			}
		}catch(Exception e) {
			
			logger.info("Exception in processElement method in RemoveHeader");
		}
		logger.info("Completed processElement method in RemoveHeader");
		
	}
}
