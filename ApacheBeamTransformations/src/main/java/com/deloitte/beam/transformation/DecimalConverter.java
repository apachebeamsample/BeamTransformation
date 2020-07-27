package com.deloitte.beam.transformation;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DecimalConverter extends DoFn<String, String> {
	private static final Logger logger = LoggerFactory.getLogger(DecimalConverter.class);

	@ProcessElement
	public void processElement(ProcessContext c) {
		logger.info("Starting processElement method in DecimalConverter");
		try {
			String row = (String) c.element();
			String[] cols = row.split(",");
			String colsString0 = "";
			String colsString1 = "";
			String colsString2 = "";

			if (cols[0].contains(".")) { //Reading the string and getting the string before decimal point
				colsString0 = getDecimal(cols[0].toString());
			} else {
				colsString0 = cols[0].toString();
			}
			if (cols[1].contains(".")) {
				colsString1 = getDecimal(cols[1].toString());
			} else {
				colsString1 = cols[1].toString();
			}
			if (cols[2].contains(".")) {
				colsString2 = getDecimal(cols[2].toString());
			} else {
				colsString2 = cols[2].toString();
			}
			c.output(colsString0 + "," + colsString1 + "," + colsString2);
		}catch(Exception e) {
			logger.info("Exception processElement method in DecimalConverter",e);

		}
		logger.info("Completed processElement method in DecimalConverter");

	}

	static String getDecimal(String cols) {
		String[] str = cols.split("\\.");
		String finalString = str[0];
		return finalString;

	}
}