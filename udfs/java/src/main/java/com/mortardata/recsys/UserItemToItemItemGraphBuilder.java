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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.tools.pigstats.PigStatusReporter;

import com.google.common.collect.ImmutableList;

public class UserItemToItemItemGraphBuilder extends EvalFunc<DataBag> {
    private static final TupleFactory tf = TupleFactory.getInstance();
    private static final BagFactory bf = BagFactory.getInstance();

    /**
     * For a single user, takes a bag of weighted user-item links and creates
     * a bag of weighted item-item links.
     * 
     * Input Schema:  { (user: chararray, item: chararray, weight: float) }
     * Output Schema: { (item_A: chararray, item_B: chararray, weight: float) }
     */
    public UserItemToItemItemGraphBuilder() {}

    public Schema outputSchema(Schema input) {
        try {
            ArrayList<Schema.FieldSchema> tupleFields = new ArrayList<Schema.FieldSchema>(3);
            tupleFields.add(new Schema.FieldSchema("item_A", DataType.CHARARRAY));
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

    public DataBag exec(Tuple input) throws IOException {
        DataBag inputBag = (DataBag) input.get(0);
        DataBag outputBag = bf.newDefaultBag();
        PigStatusReporter reporter = PigStatusReporter.getInstance();

        //Join the set of input items with itself and create a link
        //for each pairing with the minimum user-item weight for
        //the corresponding items.
        int i = 0;
        for (Tuple u : inputBag) {
            int j = 0;
            for (Tuple v : inputBag) {
                if (j > i) {
                    Float weight
                        = new Float(Math.min(
                            (Float) u.get(2),
                            (Float) v.get(2))
                        );

                    outputBag.add(
                        tf.newTupleNoCopy(
                            ImmutableList.of(
                                u.get(1),
                                v.get(1),
                                weight
                            )
                        )
                    );

                    outputBag.add(
                        tf.newTupleNoCopy(
                            ImmutableList.of(
                                v.get(1),
                                u.get(1),
                                weight
                            )
                        )
                    );
                }
                j++;
            }
            i++;
            
            if (reporter != null) {
                reporter.progress();
            }
        }

        return outputBag;
    }
}
