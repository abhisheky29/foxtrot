/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.server.resources;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import io.dropwizard.testing.junit.ResourceTestRule;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import com.fasterxml.jackson.databind.jsontype.impl.StdSubtypeResolver;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.querystore.impl.TableMapStore;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;

/**
 * Created by rishabh.goyal on 05/05/14.
 */
public class AnalyticsResourceTest {

    private TableMetadataManager tableMetadataManager;
    private MockElasticsearchServer elasticsearchServer;
    private HazelcastInstance hazelcastInstance;
    private QueryExecutor queryExecutor;
    private ObjectMapper mapper;
    
    public ResourceTestRule resources;
    
    @Rule
    public ResourceTestRule getResourcesTestRule() throws Exception {
    	resources = ResourceTestRule.builder().addResource(new AnalyticsResource(queryExecutor)).setMapper(mapper).build();
    	return resources;
    }
    
    public AnalyticsResourceTest() throws Exception {
        mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        SubtypeResolver subtypeResolver = new StdSubtypeResolver();
        mapper.setSubtypeResolver(subtypeResolver);
        mapper.registerSubtypes(GroupRequest.class);
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

        Settings indexSettings = ImmutableSettings.settingsBuilder().put("number_of_replicas", 0).build();
        CreateIndexRequest createRequest = new CreateIndexRequest(TableMapStore.TABLE_META_INDEX).settings(indexSettings);
        elasticsearchServer.getClient().admin().indices().create(createRequest).actionGet();
        elasticsearchServer.getClient().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        tableMetadataManager.start();
        when(tableMetadataManager.exists(anyString())).thenReturn(true);


        AnalyticsLoader analyticsLoader = new AnalyticsLoader(dataStore, elasticsearchConnection);
        TestUtils.registerActions(analyticsLoader, mapper);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService);
        List<Document> documents = TestUtils.getGroupDocuments(mapper);
        new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore)
                .save(TestUtils.TEST_TABLE, documents);
        for (Document document : documents) {
            elasticsearchServer.getClient().admin().indices()
                    .prepareRefresh(ElasticsearchUtils.getCurrentIndex(TestUtils.TEST_TABLE, document.getTimestamp()))
                    .setForce(true).execute().actionGet();
        }
    }

    @After
    public void tearDown() throws Exception {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
        tableMetadataManager.stop();
    }

    @Test
    public void testRunSync() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        Map<String, Object> expectedResponse = new LinkedHashMap<String, Object>();

        final Map<String, Object> nexusResponse = new LinkedHashMap<String, Object>(){{ put("1", 2); put("2", 2); put("3", 1); }};
        final Map<String, Object> galaxyResponse = new LinkedHashMap<String, Object>(){{ put("2", 1); put("3", 1); }};
        expectedResponse.put("android", new LinkedHashMap<String, Object>() {{
            put("nexus", nexusResponse);
            put("galaxy", galaxyResponse);
        }});

        final Map<String, Object> nexusResponse2 = new LinkedHashMap<String, Object>(){{ put("2", 1);}};
        final Map<String, Object> iPadResponse = new LinkedHashMap<String, Object>(){{ put("2", 2); }};
        final Map<String, Object> iPhoneResponse = new LinkedHashMap<String, Object>(){{ put("1", 1); }};
        expectedResponse.put("ios", new LinkedHashMap<String, Object>() {{
            put("nexus", nexusResponse2);
            put("ipad", iPadResponse);
            put("iphone", iPhoneResponse);
        }});

        GroupResponse response = resources.client().target("/v1/analytics").request().post(Entity.entity(groupRequest, MediaType.APPLICATION_JSON_TYPE), GroupResponse.class);
        assertEquals(expectedResponse, response.getResult());
    }

    @Test
    public void testRunSyncInvalidTable() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE + "-dummy");
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

    	Response response = resources.client().target("/v1/analytics").request().post(Entity.entity(groupRequest, MediaType.APPLICATION_JSON_TYPE));
    	assertEquals(response.getStatus(), 400);
    }

    @Test
    public void testRunSyncAsync() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        Map<String, Object> expectedResponse = new LinkedHashMap<String, Object>();

        final Map<String, Object> nexusResponse = new LinkedHashMap<String, Object>(){{ put("1", 2); put("2", 2); put("3", 1); }};
        final Map<String, Object> galaxyResponse = new LinkedHashMap<String, Object>(){{ put("2", 1); put("3", 1); }};
        expectedResponse.put("android", new LinkedHashMap<String, Object>() {{
            put("nexus", nexusResponse);
            put("galaxy", galaxyResponse);
        }});

        final Map<String, Object> nexusResponse2 = new LinkedHashMap<String, Object>(){{ put("2", 1);}};
        final Map<String, Object> iPadResponse = new LinkedHashMap<String, Object>(){{ put("2", 2); }};
        final Map<String, Object> iPhoneResponse = new LinkedHashMap<String, Object>(){{ put("1", 1); }};
        expectedResponse.put("ios", new LinkedHashMap<String, Object>() {{
            put("nexus", nexusResponse2);
            put("ipad", iPadResponse);
            put("iphone", iPhoneResponse);
        }});

        AsyncDataToken response = resources.client().target("/v1/analytics/async").request().post(Entity.entity(groupRequest, MediaType.APPLICATION_JSON_TYPE), AsyncDataToken.class);
        Thread.sleep(2000);
        GroupResponse actualResponse = GroupResponse.class.cast(CacheUtils.getCacheFor(response.getAction()).get(response.getKey()));

        assertEquals(expectedResponse, actualResponse.getResult());
    }

    @Test
    public void testRunSyncAsyncInvalidTable() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE + "-dummy");
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        GroupResponse expectedResponse = new GroupResponse();
        AsyncDataToken asyncDataToken = resources.client().target("/v1/analytics/async").request().post(Entity.entity(groupRequest, MediaType.APPLICATION_JSON_TYPE), AsyncDataToken.class);
        Thread.sleep(2000);

        GroupResponse actualResponse = GroupResponse.class.cast(CacheUtils.getCacheFor(asyncDataToken.getAction()).get(asyncDataToken.getKey()));
        assertEquals(expectedResponse.getResult(), actualResponse.getResult());
    }
}
