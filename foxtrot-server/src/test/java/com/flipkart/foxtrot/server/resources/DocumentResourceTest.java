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

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import io.dropwizard.testing.junit.ResourceTestRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.glassfish.jersey.server.ContainerException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import com.fasterxml.jackson.databind.jsontype.impl.StdSubtypeResolver;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
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
 * Created by rishabh.goyal on 04/05/14.
 */
public class DocumentResourceTest {
    private JsonNodeFactory factory = JsonNodeFactory.instance;

    private TableMetadataManager tableMetadataManager;
    private MockElasticsearchServer elasticsearchServer;
    private HazelcastInstance hazelcastInstance;

    private QueryStore queryStore;
    
    private ObjectMapper mapper;
    public ResourceTestRule resources;
    
    @Rule
    public ResourceTestRule getResourcesTestRule() throws Exception {
    	resources = ResourceTestRule.builder().addResource(new DocumentResource(queryStore)).setMapper(mapper).build();
    	return resources;
    }

    public DocumentResourceTest() throws Exception {
    	mapper = new ObjectMapper();
        
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
        queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore);
        queryStore = spy(queryStore);
    }

    @After
    public void tearDown() throws Exception {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
        tableMetadataManager.stop();
    }


    @Test
    public void testSaveDocument() throws Exception {
        String id = UUID.randomUUID().toString();
        Document document = new Document(
                id,
                System.currentTimeMillis(),
                factory.objectNode().put("hello", "world"));
        resources.client().target("/v1/document/" + TestUtils.TEST_TABLE).request().post(Entity.entity(document, MediaType.APPLICATION_JSON_TYPE));
        Document response = queryStore.get(TestUtils.TEST_TABLE, id);
        compare(document, response);
    }

    @Test
    public void testSaveDocumentInternalError() throws Exception {
        String id = UUID.randomUUID().toString();
        Document document = new Document(
                id,
                System.currentTimeMillis(),
                factory.objectNode().put("hello", "world"));
        doThrow(new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_SAVE_ERROR, "Dummy Exception"))
                .when(queryStore).save(anyString(), Matchers.<Document>any());
    	Response response = resources.client().target("/v1/document/" + TestUtils.TEST_TABLE).request().post(Entity.entity(document, MediaType.APPLICATION_JSON_TYPE));
    	assertEquals(response.getStatus(), 500);
    }

    @Test(expected = ProcessingException.class)
    public void testSaveDocumentNullId() throws Exception {
        Document document = new Document(
                null,
                System.currentTimeMillis(),
                factory.objectNode().put("hello", "world"));
        resources.client().target("/v1/document/" + TestUtils.TEST_TABLE).request().post(Entity.entity(document, MediaType.APPLICATION_JSON_TYPE));
    }

    @Test
    public void testSaveDocumentNullData() throws Exception {
        Document document = new Document(
                UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                null);
        Response response = resources.client().target("/v1/document/" + TestUtils.TEST_TABLE).request().post(Entity.entity(document, MediaType.APPLICATION_JSON_TYPE));
        assertEquals(response.getStatus(), 500);
    }

    @Test(expected = ProcessingException.class)
    public void testSaveDocumentUnknownObject() throws Exception {
    	resources.client().target("/v1/document/" + TestUtils.TEST_TABLE).request().post(Entity.entity("hello", MediaType.APPLICATION_JSON_TYPE));
    }

    @Test(expected = ProcessingException.class)
    public void testSaveDocumentEmptyJson() throws Exception {
    	resources.client().target("/v1/document/" + TestUtils.TEST_TABLE).request().post(Entity.entity("{}", MediaType.APPLICATION_JSON_TYPE));
    }


    @Test
    public void testSaveDocuments() throws Exception {
        List<Document> documents = new ArrayList<Document>();
        String id1 = UUID.randomUUID().toString();
        Document document1 = new Document(id1, System.currentTimeMillis(), factory.objectNode().put("D", "data"));
        String id2 = UUID.randomUUID().toString();
        Document document2 = new Document(id2, System.currentTimeMillis(), factory.objectNode().put("D", "data"));
        documents.add(document1);
        documents.add(document2);
        resources.client().target(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE)).request().post(Entity.entity(documents, MediaType.APPLICATION_JSON_TYPE));

        compare(document1, queryStore.get(TestUtils.TEST_TABLE, id1));
        compare(document2, queryStore.get(TestUtils.TEST_TABLE, id2));
    }

    @Test
    public void testSaveDocumentsInternalError() throws Exception {
        List<Document> documents = new ArrayList<Document>();
        String id1 = UUID.randomUUID().toString();
        Document document1 = new Document(id1, System.currentTimeMillis(), factory.objectNode().put("D", "data"));
        String id2 = UUID.randomUUID().toString();
        Document document2 = new Document(id2, System.currentTimeMillis(), factory.objectNode().put("D", "data"));
        documents.add(document1);
        documents.add(document2);
        doThrow(new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_SAVE_ERROR, "Dummy Exception"))
                .when(queryStore).save(anyString(), anyListOf(Document.class));
    	Response response = resources.client().target(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE)).request().post(Entity.entity(documents, MediaType.APPLICATION_JSON_TYPE));
    	assertEquals(response.getStatus(), 500);
    }

    @Test(expected = ProcessingException.class)
    public void testSaveDocumentsNullDocuments() throws Exception {
        List<Document> documents = null;
        resources.client().target(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE)).request().post(Entity.entity(documents, MediaType.APPLICATION_JSON_TYPE));
    }

    @Test(expected=ProcessingException.class)
    public void testSaveDocumentsNullDocument() throws Exception {
        List<Document> documents = new Vector<Document>();
        documents.add(null);
        documents.add(new Document(UUID.randomUUID().toString(), System.currentTimeMillis(), factory.objectNode().put("d", "d")));
    	resources.client().target(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE)).request().post(Entity.entity(documents, MediaType.APPLICATION_JSON_TYPE));
    }

    @Test(expected=ProcessingException.class)
    public void testSaveDocumentsNullId() throws Exception {
        List<Document> documents = new Vector<Document>();
        documents.add(new Document(null, System.currentTimeMillis(), factory.objectNode().put("d", "d")));
    	resources.client().target(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE)).request().post(Entity.entity(documents, MediaType.APPLICATION_JSON_TYPE));
    }

    @Test(expected=BadRequestException.class)
    public void testSaveDocumentsNullData() throws Exception {
        List<Document> documents = new Vector<Document>();
        documents.add(new Document(UUID.randomUUID().toString(), System.currentTimeMillis(), null));
    	Response response = resources.client().target(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE)).request().post(Entity.entity(documents, MediaType.APPLICATION_JSON_TYPE));
//    	assertEquals(response.getStatus(), 201);
    }

    @Test(expected = ProcessingException.class)
    public void testSaveDocumentsInvalidRequestObject() throws Exception {
        resources.client().target(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE)).request().post(Entity.entity("Hello", MediaType.APPLICATION_JSON_TYPE));
    }

    @Test
    public void testSaveDocumentsEmptyList() throws Exception {
        Response response = resources.client().target(String.format("/v1/document/%s/bulk", TestUtils.TEST_TABLE)).request().post(Entity.entity("[]", MediaType.APPLICATION_JSON_TYPE));
        assertEquals(response.getStatus(), 400);
    }

    @Test
    public void testGetDocument() throws Exception {
        String id = UUID.randomUUID().toString();
        Document document = new Document(id, System.currentTimeMillis(), factory.objectNode().put("D", "data"));
        queryStore.save(TestUtils.TEST_TABLE, document);

        Document response = resources.client().target(String.format("/v1/document/%s/%s", TestUtils.TEST_TABLE, id)).request().get(Document.class);
        compare(document, response);
    }

    @Test(expected=NotFoundException.class)
    public void testGetDocumentMissingId() throws Exception {
        String id = UUID.randomUUID().toString();
        resources.client().target(String.format("/v1/document/%s/%s", TestUtils.TEST_TABLE, id)).request().get(Document.class);
    }

    @Test(expected=InternalServerErrorException.class)
    public void testGetDocumentInternalError() throws Exception {
        String id = UUID.randomUUID().toString();
        doThrow(new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_GET_ERROR, "Error"))
                .when(queryStore).get(anyString(), anyString());
        resources.client().target(String.format("/v1/document/%s/%s", TestUtils.TEST_TABLE, id)).request().get(Document.class);
    }


    @Test
    public void testGetDocuments() throws Exception {
        List<Document> documents = new ArrayList<Document>();
        String id1 = UUID.randomUUID().toString();
        Document document1 = new Document(id1, System.currentTimeMillis(), factory.objectNode().put("D", "data"));
        String id2 = UUID.randomUUID().toString();
        Document document2 = new Document(id2, System.currentTimeMillis(), factory.objectNode().put("D", "data"));
        documents.add(document1);
        documents.add(document2);
        queryStore.save(TestUtils.TEST_TABLE, documents);
        String response = resources.client().target(String.format("/v1/document/%s", TestUtils.TEST_TABLE))
                .queryParam("id", id1)
                .queryParam("id", id2).request()
                .get(String.class);

        String expectedResponse = mapper.writeValueAsString(documents);
        assertEquals(expectedResponse, response);
    }

    @Test
    public void testGetDocumentsNoIds() throws Exception {
        String response = resources.client().target(String.format("/v1/document/%s", TestUtils.TEST_TABLE)).request().get(String.class);
        String expectedResponse = mapper.writeValueAsString(new ArrayList<Document>());
        assertEquals(expectedResponse, response);
    }

    @Test(expected=NotFoundException.class)
    public void testGetDocumentsMissingIds() throws Exception {
        resources.client().target(String.format("/v1/document/%s", TestUtils.TEST_TABLE))
                .queryParam("id", UUID.randomUUID().toString()).request().get(String.class);
    }

    @Test(expected=InternalServerErrorException.class)
    public void testGetDocumentsInternalError() throws Exception {
        doThrow(new QueryStoreException(QueryStoreException.ErrorCode.DOCUMENT_GET_ERROR, "Error"))
                .when(queryStore).get(anyString(), anyListOf(String.class));
        resources.client().target(String.format("/v1/document/%s", TestUtils.TEST_TABLE))
                .queryParam("id", UUID.randomUUID().toString()).request()
                .get(String.class);
    }


    public void compare(Document expected, Document actual) throws Exception {
        assertNotNull(expected);
        assertNotNull(actual);
        assertNotNull("Actual document Id should not be null", actual.getId());
        assertNotNull("Actual document data should not be null", actual.getData());
        assertEquals("Actual Doc Id should match expected Doc Id", expected.getId(), actual.getId());
        assertEquals("Actual Doc Timestamp should match expected Doc Timestamp", expected.getTimestamp(), actual.getTimestamp());
        Map<String, Object> expectedMap = mapper.convertValue(expected.getData(), new TypeReference<HashMap<String, Object>>() {});
        Map<String, Object> actualMap = mapper.convertValue(actual.getData(), new TypeReference<HashMap<String, Object>>() {});
        assertEquals("Actual data should match expected data", expectedMap, actualMap);
    }
}
