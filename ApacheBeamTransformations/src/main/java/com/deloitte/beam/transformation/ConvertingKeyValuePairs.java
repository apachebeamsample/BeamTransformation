package com.deloitte.beam.transformation;

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertingKeyValuePairs extends SimpleFunction<String, KV<String, Integer>>

{
	private static final Logger logger = LoggerFactory.getLogger(ConvertingKeyValuePairs.class);
	String key = "";
	Integer value = 0;

	@Override
	public KV<String, Integer> apply(String input) {
		logger.info("Starting apply method in ConvertingKeyValuePairs");
		try {

			String[] split = input.split(","); //split the i/p string
			if (split.length < 2) {
				return null;
			}
			key = split[0];
			value = Integer.valueOf(split[1]);

		} catch (Exception e) {
			logger.error("Exception in  apply method in ConvertingKeyValuePairs",e);
		}
		logger.info("Completed apply method in ConvertingKeyValuePairs");
		return KV.of(key, value);

	}
}
