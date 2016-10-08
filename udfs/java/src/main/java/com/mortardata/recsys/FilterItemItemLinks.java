/*
 * Copyright 2014 Mortar Data Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "as is" Basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mortardata.recsys;

import gnu.trove.iterator.TObjectFloatIterator;
import gnu.trove.map.hash.TObjectFloatHashMap;

import java.util.ArrayList;

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.google.common.collect.ImmutableList;


public class FilterItemItemLinks extends EvalFunc<DataBag> implements Accumulator<DataBag> {
    private static final TupleFactory tf = TupleFactory.getInstance();
    private static final BagFactory bf = BagFactory.getInstance();

    private float minLinkWeight;
    private TObjectFloatHashMap<String> inputItems;
    private DataBag outputItems;

    /**
     * For a single item_A, this UDF takes a bag of weighted item-item links and returns a bag of 
     * (item_B, weight) tuples that are above a minimum weight.
     * 
     * The input bag should have a common item_A (results of a group on item_A) so
     * item_A isn't returned because the caller can easily add it back.
     *  
     * Input Schema:  { (item_A: chararray, item_B: chararray, weight: float) }
     * Output Schema: { (item_B: chararray, weight: float) }
     *
     * @param minLinkWeight: Any item-item link with a weight less than this will be removed.
     */
    public FilterItemItemLinks(String minLinkWeight) {
        this.minLinkWeight = Float.parseFloat(minLinkWeight);
        cleanup();
    }
    
    public Schema outputSchema(Schema input) {
        try {
            ArrayList<FieldSchema> tupleFields = new ArrayList<FieldSchema>(2);
            tupleFields.add(new Schema.FieldSchema("item_B", DataType.CHARARRAY));
            tupleFields.add(new Schema.FieldSchema("weight", DataType.FLOAT));
            
            return new Schema(
                new Schema.FieldSchema("ii_terms",
                    new Schema(
                        new Schema.FieldSchema(null,
                            new Schema(tupleFields),
                        DataType.TUPLE)),
                DataType.BAG)
            );
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    public DataBag exec(Tuple input) {
        accumulate(input);
        DataBag output = getValue();
        cleanup();
        return output;
    }



    public void cleanup() {
        inputItems = new TObjectFloatHashMap<String>();
        outputItems = bf.newDefaultBag();
    }

    public DataBag getValue() {
        TObjectFloatIterator<String> it = inputItems.iterator();
        while (it.hasNext()) {
            it.advance();
            if (it.value() >= minLinkWeight) {
                outputItems.add(tf.newTupleNoCopy(
                    ImmutableList.of(it.key(), it.value())
                ));
            }
        }

        return outputItems;
    }

    /**
     * @param input:  Bag of (item_A: chararray, item_B: chararray, weight: float) tuples
     *                  with common item_A
     */
    public void accumulate(Tuple input) {
        try {
            DataBag inputBag = (DataBag) input.get(0);

            for (Tuple t : inputBag) {
                String item = (String) t.get(1);
                float weight = (Float) t.get(2);
                inputItems.adjustOrPutValue(item, weight, weight);
            }
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }
}
