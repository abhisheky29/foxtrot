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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import io.dropwizard.testing.junit.ResourceTestRule;

import java.util.UUID;

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
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.flipkart.foxtrot.core.querystore.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.querystore.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.querystore.impl.TableMapStore;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;

/**
 * Created by rishabh.goyal on 04/05/14.
 */
public class TableMetadataResourceTest {

    private TableMetadataManager tableMetadataManager;
    private MockElasticsearchServer elasticsearchServer;
    private HazelcastInstance hazelcastInstance;
    
    public ResourceTestRule resources;
    private ObjectMapper mapper;
    
    @Rule
    public ResourceTestRule getResourcesTestRule() throws Exception {
    	resources = ResourceTestRule.builder().addResource(new TableMetadataResource(tableMetadataManager)).setMapper(mapper).build();
    	return resources;
    }

    public TableMetadataResourceTest() throws Exception {
        mapper = new ObjectMapper();
        ElasticsearchUtils.setMapper(mapper);

        //Initializing Cache Factory
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        CacheUtils.setCacheFactory(new DistributedCacheFactory(hazelcastConnection, mapper));
        Config config = new Config();
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(config);
        elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());
        ElasticsearchUtils.initializeMappings(elasticsearchServer.getClient());

        Settings indexSettings = ImmutableSettings.settingsBuilder().put("number_of_replicas", 0).build();
        CreateIndexRequest createRequest = new CreateIndexRequest(TableMapStore.TABLE_META_INDEX).settings(indexSettings);
        elasticsearchServer.getClient().admin().indices().create(createRequest).actionGet();
        elasticsearchServer.getClient().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection);
        tableMetadataManager = spy(tableMetadataManager);
        tableMetadataManager.start();
    }

    @After
    public void tearDown() throws Exception {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
        tableMetadataManager.stop();
    }


    @Test
    public void testSave() throws Exception {
        Table table = new Table(TestUtils.TEST_TABLE, 30);
        resources.client().target("/v1/tables").request().post(Entity.entity(table, MediaType.APPLICATION_JSON_TYPE));

        Table response = tableMetadataManager.get(table.getName());
        assertNotNull(response);
        assertEquals(table.getName(), response.getName());
        assertEquals(table.getTtl(), response.getTtl());
    }

    @Test(expected = ProcessingException.class)
    public void testSaveNullTable() throws Exception {
        Table table = null;
        resources.client().target("/v1/tables").request().post(Entity.entity(table, MediaType.APPLICATION_JSON_TYPE));
    }

    @Test(expected = ProcessingException.class)
    public void testSaveNullTableName() throws Exception {
        Table table = new Table(null, 30);
        resources.client().target("/v1/tables").request().post(Entity.entity(table, MediaType.APPLICATION_JSON_TYPE));
    }

    @Test
    public void testSaveBackendError() throws Exception {
        Table table = new Table(UUID.randomUUID().toString(), 30);
        doThrow(new Exception()).when(tableMetadataManager).save(Matchers.<Table>any());
        Response response = resources.client().target("/v1/tables").request().post(Entity.entity(table, MediaType.APPLICATION_JSON_TYPE));
        assertEquals(response.getStatus(), 500);
    }

    @Test(expected = ProcessingException.class)
    public void testSaveIllegalTtl() throws Exception {
        Table table = new Table(TestUtils.TEST_TABLE, 0);
        resources.client().target("/v1/tables").request().post(Entity.entity(table, MediaType.APPLICATION_JSON_TYPE));
    }


    @Test
    public void testGet() throws Exception {
        Table table = new Table(TestUtils.TEST_TABLE, 30);
        tableMetadataManager.save(table);

        Table response = resources.client().target(String.format("/v1/tables/%s", table.getName())).request().get(Table.class);
        assertNotNull(response);
        assertEquals(table.getName(), response.getName());
        assertEquals(table.getTtl(), response.getTtl());
    }

    @Test(expected=NotFoundException.class)
    public void testGetMissingTable() throws Exception {
    	resources.client().target(String.format("/v1/tables/%s", TestUtils.TEST_TABLE)).request().get(Table.class);
    }
}
