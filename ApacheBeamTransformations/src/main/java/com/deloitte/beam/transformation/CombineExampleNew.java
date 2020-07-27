package com.deloitte.beam.transformation;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//Combine is a Beam transform for combining collections of elements or values in your data. 
public class CombineExampleNew {
	private static final Logger logger = LoggerFactory.getLogger(CombineExampleNew.class);
	
	public static void main(String args[]) {
		logger.info("Starting main method in CombineExampleNew");
		try {
			Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());
			PCollection<Integer> pc = p.apply(Create.of(3, 4, 5)); //Creating PCollection Of Integers
			PCollection<Integer> sum = pc.apply(Combine.globally(new SumIntegers())); //Calling sum of Integers method
			sum.apply(ParDo.of(new DoFn<Integer, Void>() {
				@ProcessElement
				public void processElement(ProcessContext c) {
					System.out.println(c.element()); //Printing the sum
				}
			}));
			p.run().waitUntilFinish();
		}catch(Exception e) {
			logger.error("Exception in main method in CombineExampleNew",e);
		}
		
		logger.info("Completed main method in CombineExampleNew");
	}

}
