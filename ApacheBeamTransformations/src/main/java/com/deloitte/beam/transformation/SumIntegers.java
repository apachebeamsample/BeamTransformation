package com.deloitte.beam.transformation;

import org.apache.beam.sdk.transforms.SerializableFunction;

public class SumIntegers implements SerializableFunction<Iterable<Integer>, Integer> {
    @Override
    public Integer apply(Iterable<Integer> input) {
        Integer sum = 0;
        for (Integer item : input) {
            sum += item;
        }
       
        return sum;
    }
}