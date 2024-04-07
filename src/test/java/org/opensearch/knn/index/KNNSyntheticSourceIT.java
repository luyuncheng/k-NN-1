/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.knn.index;

import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.knn.KNNRestTestCase;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.index.query.KNNQueryBuilder;
import org.opensearch.knn.index.util.KNNEngine;

import java.io.IOException;
import java.util.Map;

public class KNNSyntheticSourceIT extends KNNRestTestCase {

    public void testSyntheticSourceInSearch() throws IOException, ParseException {
        String indexNameWithSynthetic = "test-index-synthetic";
        String indexNameWithoutSynthetic = "test-index-no-synthetic";
        String fieldName = "test-field-1";
        Integer dimension = 2;

        KNNMethod hnswMethod = KNNEngine.FAISS.getMethod(KNNConstants.METHOD_HNSW);
        SpaceType spaceType = SpaceType.L2;

        // Create an index
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_source")
                .startArray("excludes")
                .value(fieldName)
                .endArray()
                .endObject()
                .startObject("properties")
                .startObject(fieldName)
                .field("type", "knn_vector")
                .field("dimension", dimension)
                .startObject(KNNConstants.KNN_METHOD)
                .field(KNNConstants.NAME, hnswMethod.getMethodComponent().getName())
                .field(KNNConstants.METHOD_PARAMETER_SPACE_TYPE, spaceType.getValue())
                .field(KNNConstants.KNN_ENGINE, KNNEngine.FAISS.getName())
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        String mapping = builder.toString();

        Settings indexSettingWithSynthetic = Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .put("index.knn.synthetic_source.enabled", true)
                .put("index.knn", true).build();

        Settings indexSettingWithoutSynthetic = Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .put("index.knn.synthetic_source.enabled", false)
                .put("index.knn", true).build();

        createKnnIndex(indexNameWithSynthetic, indexSettingWithSynthetic, mapping);
        createKnnIndex(indexNameWithoutSynthetic, indexSettingWithoutSynthetic, mapping);

        Float[] vector = { 6.0f, 6.0f };
        addKnnDoc(indexNameWithSynthetic, "1", fieldName, vector);
        addKnnDoc(indexNameWithoutSynthetic, "1", fieldName, vector);
        float[] queryVector = { 6.0f, 6.0f};

        Response responseWithSynthetic = searchKNNIndex(indexNameWithSynthetic, new KNNQueryBuilder(fieldName, queryVector, 10), 10);
        String resp1 = EntityUtils.toString(responseWithSynthetic.getEntity());
        assertTrue(resp1.contains("\"test-field-1\":[\"6.0\",\"6.0\"]"));

        Response responseWithoutSynthetic = searchKNNIndex(indexNameWithoutSynthetic, new KNNQueryBuilder(fieldName, queryVector, 10), 10);
        String resp2 = EntityUtils.toString(responseWithoutSynthetic.getEntity());
        assertFalse(resp2.contains("\"test-field-1\":[\"6.0\",\"6.0\"]"));
    }

    public void testSyntheticSourceReindex() throws IOException, ParseException {
        String indexNameWithSynthetic = "test-index-synthetic";
        String indexNameWithoutSynthetic = "test-index-no-synthetic";
        String reindexNameWithSynthetic = "test-reindex-synthetic";
        String reindexNameWithoutSynthetic = "test-reindex-no-synthetic";
        String fieldName = "test-field-1";
        Integer dimension = 2;

        KNNMethod hnswMethod = KNNEngine.FAISS.getMethod(KNNConstants.METHOD_HNSW);
        SpaceType spaceType = SpaceType.L2;

        // Create an index
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_source")
                .startArray("excludes")
                .value(fieldName)
                .endArray()
                .endObject()
                .startObject("properties")
                .startObject(fieldName)
                .field("type", "knn_vector")
                .field("dimension", dimension)
                .startObject(KNNConstants.KNN_METHOD)
                .field(KNNConstants.NAME, hnswMethod.getMethodComponent().getName())
                .field(KNNConstants.METHOD_PARAMETER_SPACE_TYPE, spaceType.getValue())
                .field(KNNConstants.KNN_ENGINE, KNNEngine.FAISS.getName())
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        String mapping = builder.toString();

        Settings indexSettingWithSynthetic = Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .put("index.knn.synthetic_source.enabled", true)
                .put("index.knn", true).build();

        Settings indexSettingWithoutSynthetic = Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .put("index.knn.synthetic_source.enabled", false)
                .put("index.knn", true).build();

        createKnnIndex(indexNameWithSynthetic, indexSettingWithSynthetic, mapping);
        createKnnIndex(reindexNameWithSynthetic, indexSettingWithSynthetic, mapping);
        createKnnIndex(indexNameWithoutSynthetic, indexSettingWithoutSynthetic, mapping);
        createKnnIndex(reindexNameWithoutSynthetic, indexSettingWithoutSynthetic, mapping);

        Float[] vector = { 6.0f, 6.0f };
        addKnnDoc(indexNameWithSynthetic, "1", fieldName, vector);
        addKnnDoc(indexNameWithoutSynthetic, "1", fieldName, vector);
        float[] queryVector = { 6.0f, 6.0f};

        doReindex(indexNameWithSynthetic, reindexNameWithSynthetic);
        doReindex(indexNameWithoutSynthetic, reindexNameWithoutSynthetic);

        Response responseWithSynthetic = searchKNNIndex(reindexNameWithSynthetic, new KNNQueryBuilder(fieldName, queryVector, 10), 10);
        String resp1 = EntityUtils.toString(responseWithSynthetic.getEntity());
        System.out.println("=====" + resp1);
        assertTrue(resp1.contains("\"test-field-1\":[\"6.0\",\"6.0\"]"));

        Response responseWithoutSynthetic = searchKNNIndex(reindexNameWithoutSynthetic, new KNNQueryBuilder(fieldName, queryVector, 10), 10);
        String resp2 = EntityUtils.toString(responseWithoutSynthetic.getEntity());
        System.out.println("=====" + resp2);
        assertFalse(resp2.contains("\"test-field-1\":[\"6.0\",\"6.0\"]"));
    }
}
