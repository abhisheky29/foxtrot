package com.flipkart.foxtrot.core.querystore.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.histogram.Period;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.numeric.GreaterThanFilter;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 28/04/14.
 */
public class HistogramActionTest {
    private QueryExecutor queryExecutor;
    private final ObjectMapper mapper = new ObjectMapper();
    private MockElasticsearchServer elasticsearchServer;
    private HazelcastInstance hazelcastInstance;
    private JsonNodeFactory factory = JsonNodeFactory.instance;

    @Before
    public void setUp() throws Exception {
        ElasticsearchUtils.setMapper(mapper);
        DataStore dataStore = TestUtils.getDataStore();

        //Initializing Cache Factory
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        CacheUtils.setCacheFactory(new DistributedCacheFactory(hazelcastConnection, mapper));

        elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        ElasticsearchUtils.initializeMappings(elasticsearchServer.getClient());

        // Ensure that table exists before saving/reading data from it
        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        when(tableMetadataManager.exists(TestUtils.TEST_TABLE)).thenReturn(true);

        AnalyticsLoader analyticsLoader = new AnalyticsLoader(dataStore, elasticsearchConnection);
        TestUtils.registerActions(analyticsLoader, mapper);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService);
        new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, queryExecutor)
                .save(TestUtils.TEST_TABLE, TestUtils.getHistogramDocuments(mapper));
    }

    @After
    public void tearDown() throws IOException {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }

    @Test(expected = QueryStoreException.class)
    public void testHistogramActionAnyException() throws QueryStoreException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE);
        histogramRequest.setPeriod(Period.minutes);
        histogramRequest.setFrom(0);
        histogramRequest.setField("_timestamp");
        when(elasticsearchServer.getClient()).thenReturn(null);
        queryExecutor.execute(histogramRequest);
    }

    // TODO Need to correct this test case. Date fields are converting to long due to jsonnode
    @Ignore
    @Test(expected = Exception.class)
    public void testHistogramActionFieldWithSpecialCharacterNoFilter() throws QueryStoreException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE);
        histogramRequest.setPeriod(Period.minutes);
        histogramRequest.setFrom(0);
        histogramRequest.setField("header.timestamp");

        ArrayNode countsNode = factory.arrayNode();
        countsNode.add(factory.objectNode().put("period", 1398653100000L).put("count", 1));
        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "histogram");
        finalNode.put("counts", countsNode);

        String expectedResponse = mapper.writeValueAsString(finalNode);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(histogramRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testHistogramActionIntervalMinuteNoFilter() throws QueryStoreException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE);
        histogramRequest.setPeriod(Period.minutes);
        histogramRequest.setFrom(0);
        histogramRequest.setField("_timestamp");

        ArrayNode countsNode = factory.arrayNode();
        countsNode.add(factory.objectNode().put("period", 1397651100000L).put("count", 2));
        countsNode.add(factory.objectNode().put("period", 1397658060000L).put("count", 3));
        countsNode.add(factory.objectNode().put("period", 1397658180000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1397758200000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1397958060000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1398653100000L).put("count", 2));
        countsNode.add(factory.objectNode().put("period", 1398658200000L).put("count", 1));

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "histogram");
        finalNode.put("counts", countsNode);

        String expectedResponse = mapper.writeValueAsString(finalNode);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(histogramRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testHistogramActionIntervalMinuteWithFilter() throws QueryStoreException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE);
        histogramRequest.setPeriod(Period.minutes);
        histogramRequest.setFrom(0);
        histogramRequest.setField("_timestamp");

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        histogramRequest.setFilters(Collections.<Filter>singletonList(greaterThanFilter));

        ArrayNode countsNode = factory.arrayNode();
        countsNode.add(factory.objectNode().put("period", 1397651100000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1397658060000L).put("count", 2));
        countsNode.add(factory.objectNode().put("period", 1397658180000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1397958060000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1398658200000L).put("count", 1));
        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "histogram");
        finalNode.put("counts", countsNode);

        String expectedResponse = mapper.writeValueAsString(finalNode);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(histogramRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testHistogramActionIntervalHourNoFilter() throws QueryStoreException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE);
        histogramRequest.setPeriod(Period.hours);
        histogramRequest.setFrom(0);
        histogramRequest.setField("_timestamp");

        ArrayNode countsNode = factory.arrayNode();
        countsNode.add(factory.objectNode().put("period", 1397649600000L).put("count", 2));
        countsNode.add(factory.objectNode().put("period", 1397656800000L).put("count", 4));
        countsNode.add(factory.objectNode().put("period", 1397757600000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1397955600000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1398650400000L).put("count", 2));
        countsNode.add(factory.objectNode().put("period", 1398657600000L).put("count", 1));

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "histogram");
        finalNode.put("counts", countsNode);

        String expectedResponse = mapper.writeValueAsString(finalNode);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(histogramRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testHistogramActionIntervalHourWithFilter() throws QueryStoreException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE);
        histogramRequest.setPeriod(Period.hours);
        histogramRequest.setFrom(0);
        histogramRequest.setField("_timestamp");

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        histogramRequest.setFilters(Collections.<Filter>singletonList(greaterThanFilter));

        ArrayNode countsNode = factory.arrayNode();
        countsNode.add(factory.objectNode().put("period", 1397649600000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1397656800000L).put("count", 3));
        countsNode.add(factory.objectNode().put("period", 1397955600000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1398657600000L).put("count", 1));

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "histogram");
        finalNode.put("counts", countsNode);

        String expectedResponse = mapper.writeValueAsString(finalNode);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(histogramRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testHistogramActionIntervalDayNoFilter() throws QueryStoreException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE);
        histogramRequest.setPeriod(Period.days);
        histogramRequest.setFrom(0);
        histogramRequest.setField("_timestamp");

        ArrayNode countsNode = factory.arrayNode();
        countsNode.add(factory.objectNode().put("period", 1397606400000L).put("count", 6));
        countsNode.add(factory.objectNode().put("period", 1397692800000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1397952000000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1398643200000L).put("count", 3));

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "histogram");
        finalNode.put("counts", countsNode);

        String expectedResponse = mapper.writeValueAsString(finalNode);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(histogramRequest));
        assertEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testHistogramActionIntervalDayWithFilter() throws QueryStoreException, JsonProcessingException {
        HistogramRequest histogramRequest = new HistogramRequest();
        histogramRequest.setTable(TestUtils.TEST_TABLE);
        histogramRequest.setPeriod(Period.days);
        histogramRequest.setFrom(0);
        histogramRequest.setField("_timestamp");

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        histogramRequest.setFilters(Collections.<Filter>singletonList(greaterThanFilter));

        ArrayNode countsNode = factory.arrayNode();
        countsNode.add(factory.objectNode().put("period", 1397606400000L).put("count", 4));
        countsNode.add(factory.objectNode().put("period", 1397952000000L).put("count", 1));
        countsNode.add(factory.objectNode().put("period", 1398643200000L).put("count", 1));

        ObjectNode finalNode = factory.objectNode();
        finalNode.put("opcode", "histogram");
        finalNode.put("counts", countsNode);

        String expectedResponse = mapper.writeValueAsString(finalNode);
        String actualResponse = mapper.writeValueAsString(queryExecutor.execute(histogramRequest));
        assertEquals(expectedResponse, actualResponse);
    }
}
