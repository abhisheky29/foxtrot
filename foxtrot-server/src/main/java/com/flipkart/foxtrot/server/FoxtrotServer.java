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
package com.flipkart.foxtrot.server;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.jetty.ConnectorFactory;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.server.SimpleServerFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;

import java.util.List;
import java.util.concurrent.ExecutorService;

import javax.servlet.FilterRegistration.Dynamic;

import net.sourceforge.cobertura.CoverageIgnore;

import org.eclipse.jetty.servlets.CrossOriginFilter;

import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import com.fasterxml.jackson.databind.jsontype.impl.StdSubtypeResolver;
import com.flipkart.foxtrot.core.common.DataDeletionManager;
import com.flipkart.foxtrot.core.common.DataDeletionManagerConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HBaseDataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseTableConnection;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.TableMetadataManager;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.ClusterConfig;
import com.flipkart.foxtrot.core.querystore.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.server.cluster.ClusterManager;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.console.ElasticsearchConsolePersistence;
import com.flipkart.foxtrot.server.providers.FlatResponseCsvProvider;
import com.flipkart.foxtrot.server.providers.FlatResponseErrorTextProvider;
import com.flipkart.foxtrot.server.providers.FlatResponseTextProvider;
import com.flipkart.foxtrot.server.resources.AnalyticsResource;
import com.flipkart.foxtrot.server.resources.AsyncResource;
import com.flipkart.foxtrot.server.resources.ClusterInfoResource;
import com.flipkart.foxtrot.server.resources.ConsoleResource;
import com.flipkart.foxtrot.server.resources.DocumentResource;
import com.flipkart.foxtrot.server.resources.ElasticSearchHealthCheck;
import com.flipkart.foxtrot.server.resources.FqlResource;
import com.flipkart.foxtrot.server.resources.TableFieldMappingResource;
import com.flipkart.foxtrot.server.resources.TableMetadataResource;
import com.flipkart.foxtrot.server.util.ManagedActionScanner;
import com.flipkart.foxtrot.sql.FqlEngine;
import com.google.common.collect.Lists;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 9:38 PM
 */

@CoverageIgnore
public class FoxtrotServer extends Application<FoxtrotServerConfiguration> {
    @Override
    public void initialize(Bootstrap<FoxtrotServerConfiguration> bootstrap) {
        bootstrap.addBundle(new AssetsBundle("/console/", "/"));
    }
    
    /**
     * Returns the running port
     * 
     * @param configuration
     * @return
     */
    protected int getPort(FoxtrotServerConfiguration configuration) {
    	if (configuration.getServerFactory() instanceof DefaultServerFactory) {
	    	DefaultServerFactory serverFactory = (DefaultServerFactory) configuration.getServerFactory();
	    	for (ConnectorFactory connector : serverFactory.getApplicationConnectors()) {
	    	    if (connector.getClass().isAssignableFrom(HttpConnectorFactory.class)) {
	    	        return ((HttpConnectorFactory) connector).getPort();
	    	    }
	    	}
    	} else {
    		SimpleServerFactory serverFactory = (SimpleServerFactory) configuration.getServerFactory();
    		HttpConnectorFactory connector = (HttpConnectorFactory) serverFactory.getConnector();
    		if (connector.getClass().isAssignableFrom(HttpConnectorFactory.class)) {
    		    return connector.getPort();
    		}
    	}
    	return 0;
    }

    @Override
    public void run(FoxtrotServerConfiguration configuration, Environment environment) throws Exception {
        ObjectMapper objectMapper = environment.getObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        SubtypeResolver subtypeResolver = new StdSubtypeResolver();
        objectMapper.setSubtypeResolver(subtypeResolver);

        
        ExecutorService executorService = environment.lifecycle().scheduledExecutorService("query-executor-%s").threads(40).shutdownTime(Duration.seconds(30)).build();

        HbaseConfig hbaseConfig = configuration.getHbase();
        HbaseTableConnection HBaseTableConnection = new HbaseTableConnection(hbaseConfig);

        ElasticsearchConfig elasticsearchConfig = configuration.getElasticsearch();
        ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection(elasticsearchConfig);

        ClusterConfig clusterConfig = configuration.getCluster();
        HazelcastConnection hazelcastConnection = new HazelcastConnection(clusterConfig, objectMapper);

        ElasticsearchUtils.setMapper(objectMapper);

        DataStore dataStore = new HBaseDataStore(HBaseTableConnection, objectMapper);
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(dataStore, elasticsearchConnection);

        TableMetadataManager tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection);

        QueryExecutor executor = new QueryExecutor(analyticsLoader, executorService);
        QueryStore queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore);

        DataDeletionManagerConfig dataDeletionManagerConfig = configuration.getTableDataManagerConfig();
        DataDeletionManager dataDeletionManager = new DataDeletionManager(dataDeletionManagerConfig, queryStore);

        List<HealthCheck> healthChecks = Lists.newArrayList();
        healthChecks.add(new ElasticSearchHealthCheck(elasticsearchConnection));

        ClusterManager clusterManager = new ClusterManager(
                                    hazelcastConnection, healthChecks, getPort(configuration));

        environment.lifecycle().manage(HBaseTableConnection);
        environment.lifecycle().manage(elasticsearchConnection);
        environment.lifecycle().manage(hazelcastConnection);
        environment.lifecycle().manage(tableMetadataManager);
        environment.lifecycle().manage(new ManagedActionScanner(analyticsLoader, environment));
        environment.lifecycle().manage(dataDeletionManager);
        environment.lifecycle().manage(clusterManager);

        environment.jersey().register(new DocumentResource(queryStore));
        environment.jersey().register(new AsyncResource());
        environment.jersey().register(new AnalyticsResource(executor));
        environment.jersey().register(new TableMetadataResource(tableMetadataManager));
        environment.jersey().register(new TableFieldMappingResource(queryStore));
        environment.jersey().register(new ConsoleResource(
                new ElasticsearchConsolePersistence(elasticsearchConnection, objectMapper)));
        FqlEngine fqlEngine = new FqlEngine(tableMetadataManager, queryStore, executor, objectMapper);
        environment.jersey().register(new FqlResource(fqlEngine));
        environment.jersey().register(new ClusterInfoResource(clusterManager));

        for(HealthCheck healthCheck : healthChecks) {
            environment.jersey().register(healthCheck);
        }

        environment.jersey().register(new FlatResponseTextProvider());
        environment.jersey().register(new FlatResponseCsvProvider());
        environment.jersey().register(new FlatResponseErrorTextProvider());

        Dynamic filter = environment.servlets().addFilter("cors", CrossOriginFilter.class);
        filter.getUrlPatternMappings().add("/*");
    }
}
