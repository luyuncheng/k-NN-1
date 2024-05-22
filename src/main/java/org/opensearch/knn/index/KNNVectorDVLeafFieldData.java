/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.opensearch.index.fielddata.LeafFieldData;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.mapper.DocValueFetcher;
import org.opensearch.search.DocValueFormat;

import java.io.IOException;

public class KNNVectorDVLeafFieldData implements LeafFieldData {

    private final LeafReader reader;
    private final String fieldName;
    private final VectorDataType vectorDataType;

    public KNNVectorDVLeafFieldData(LeafReader reader, String fieldName, VectorDataType vectorDataType) {
        this.reader = reader;
        this.fieldName = fieldName;
        this.vectorDataType = vectorDataType;
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public long ramBytesUsed() {
        return 0; // unknown
    }

    @Override
    public ScriptDocValues<float[]> getScriptValues() {
        try {
            BinaryDocValues values = DocValues.getBinary(reader, fieldName);
            return new KNNVectorScriptDocValues(values, fieldName, vectorDataType);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values for knn vector field: " + fieldName, e);
        }
    }

    @Override
    public SortedBinaryDocValues getBytesValues() {
        try {
            final BinaryDocValues binaryDocValues = DocValues.getBinary(reader, fieldName);
            SortedBinaryDocValues sortedBinaryDocValues = new SortedBinaryDocValues() {

                private boolean docExists = false;
                float[] floats = null;
                int pos = 0;
                BytesRefBuilder bytesRefBuilder = new BytesRefBuilder();

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    if (binaryDocValues.advanceExact(doc)) {
                        docExists = true;
                        floats = vectorDataType.getVectorFromDocValues(binaryDocValues.binaryValue());
                        pos = 0;
                        return docExists;
                    }
                    docExists = false;
                    return docExists;
                }

                @Override
                public int docValueCount() {
                    return docExists ? floats.length : 0;
                }

                @Override
                public BytesRef nextValue() throws IOException {
                    Float v = floats[pos++];
                    bytesRefBuilder.clear();
                    bytesRefBuilder.copyChars(v.toString());
                    return bytesRefBuilder.get();
                }
            };
            return sortedBinaryDocValues;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public DocValueFetcher.Leaf getLeafValueFetcher(DocValueFormat format) {
        final BinaryDocValues binaryDocValues;

        try {
            binaryDocValues = DocValues.getBinary(reader, fieldName);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load KNNDocValues from lucene", e);
        }

        return new DocValueFetcher.Leaf() {
            float[] floats;
            boolean docExists = false;

            @Override
            public boolean advanceExact(int docId) throws IOException {
                if (binaryDocValues.advanceExact(docId)) {
                    docExists = true;
                    floats = vectorDataType.getVectorFromBytesRef(binaryDocValues.binaryValue());
                    return docExists;
                }
                docExists = false;
                return docExists;
            }

            @Override
            public int docValueCount() throws IOException {
                return 1;
            }

            @Override
            public Object nextValue() throws IOException {
                return floats;
            }
        };
    }
}
