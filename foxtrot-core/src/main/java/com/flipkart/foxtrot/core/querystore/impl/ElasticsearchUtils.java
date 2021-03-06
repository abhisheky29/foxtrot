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
package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Table;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 3:46 PM
 */
public class ElasticsearchUtils {
    public static final String TYPE_NAME = "document";
    public static final String TABLENAME_PREFIX = "foxtrot";
    public static final String TABLENAME_POSTFIX = "table";
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("dd-M-yyyy");
    private static ObjectMapper mapper;

    public static void setMapper(ObjectMapper mapper) {
        ElasticsearchUtils.mapper = mapper;
    }

    public static ObjectMapper getMapper() {
        return mapper;
    }

    public static String getIndexPrefix(final String table) {
        return String.format("%s-%s-%s-", ElasticsearchUtils.TABLENAME_PREFIX, table, ElasticsearchUtils.TABLENAME_POSTFIX);
    }

    public static String[] getIndices(final String table) {
        /*long currentTime = new Date().getTime();
        String names[] = new String[30]; //TODO::USE TABLE METADATA
        for(int i = 0 ; i < 30; i++) {
            String postfix = new SimpleDateFormat("dd-M-yyyy").format(new Date(currentTime));
            names[i] = String.format("%s-%s-%s", TABLENAME_PREFIX, table, postfix);
        }*/
        return new String[]{String.format("%s-%s-%s-*",
                ElasticsearchUtils.TABLENAME_PREFIX, table, ElasticsearchUtils.TABLENAME_POSTFIX)};
    }

    public static String getCurrentIndex(final String table, long timestamp) {
        //TODO::THROW IF TIMESTAMP IS BEYOND TABLE META.TTL
        String datePostfix = new SimpleDateFormat("dd-M-yyyy").format(new Date(timestamp));
        return String.format("%s-%s-%s-%s", ElasticsearchUtils.TABLENAME_PREFIX, table,
                ElasticsearchUtils.TABLENAME_POSTFIX, datePostfix);
    }

    public static PutIndexTemplateRequest getClusterTemplateMapping(IndicesAdminClient indicesAdminClient) {
        PutIndexTemplateRequestBuilder builder = new PutIndexTemplateRequestBuilder(indicesAdminClient, "generic_template");
        builder.setTemplate("foxtrot-*");
        builder.addMapping(TYPE_NAME, "{\n" +
                "            \"_source\" : { \"enabled\" : false },\n" +
                "            \"_all\" : { \"enabled\" : false },\n" +
                "            \"_timestamp\" : { \"enabled\" : true, \"store\" : true },\n" +
                "\n" +
                "            \"dynamic_templates\" : [\n" +
                "                {\n" +
                "                    \"template_timestamp\" : {\n" +
                "                        \"match\" : \"timestamp\",\n" +
                "                        \"mapping\" : {\n" +
                "                            \"store\" : false,\n" +
                "                            \"index\" : \"not_analyzed\",\n" +
                "                            \"type\" : \"date\"\n" +
                "                        }\n" +
                "                    }\n" +
                "                },\n" +
                "                {\n" +
                "                    \"template_no_store_analyzed\" : {\n" +
                "                        \"match\" : \"*\",\n" +
                "                        \"match_mapping_type\" : \"string\",\n" +
                "                        \"mapping\" : {\n" +
                "                            \"store\" : false,\n" +
                "                            \"index\" : \"not_analyzed\",\n" +
                "                            \"fields\" : {\n" +
                "                                \"analyzed\": {\n" +
                "                                    \"store\" : false,\n" +
                "                                    \"type\": \"string\",\n" +
                "                                    \"index\": \"analyzed\"\n" +
                "                                }\n" +
                "                            }\n" +
                "                        }\n" +
                "                    }\n" +
                "                },\n" +
                "                {\n" +
                "                    \"template_no_store\" : {\n" +
                "                        \"match_mapping_type\": \"date|boolean|double|long|integer\",\n" +
                "                        \"match_pattern\": \"regex\",\n" +
                "                        \"path_match\": \".*\",\n" +
                "                        \"mapping\" : {\n" +
                "                            \"store\" : false,\n" +
                "                            \"index\" : \"not_analyzed\"\n" +
                "                        }\n" +
                "                    }\n" +
                "                }\n" +
                "            ]\n" +
                "        }");
        return builder.request();
    }

    public static void initializeMappings(Client client) {
        PutIndexTemplateRequest templateRequest = getClusterTemplateMapping(client.admin().indices());
        client.admin().indices().putTemplate(templateRequest).actionGet();
    }

    public static String getValidTableName(String table) {
        if (table == null) return null;
        return table.toLowerCase();
    }

    public static boolean isIndexValidForTable(String index, String table) {
        String indexPrefix = getIndexPrefix(table);
        return index.startsWith(indexPrefix);
    }

    public static boolean isIndexEligibleForDeletion(String index, Table table) {
        if (index == null || table == null || !isIndexValidForTable(index, table.getName())) {
            return false;
        }

        String indexPrefix = getIndexPrefix(table.getName());
        String creationDateString = index.substring(index.indexOf(indexPrefix) + indexPrefix.length());
        DateTime creationDate = DATE_TIME_FORMATTER.parseDateTime(creationDateString);
        DateTime startTime = new DateTime(0L);
        DateTime endTime = new DateTime().minusDays(table.getTtl()).toDateMidnight().toDateTime();
        return creationDate.isAfter(startTime) && creationDate.isBefore(endTime);
    }

    public static String getTableNameFromIndex(String currentIndex) {
        if (currentIndex.contains(TABLENAME_PREFIX) && currentIndex.contains(TABLENAME_POSTFIX)) {
            String tempIndex = currentIndex.substring(currentIndex.indexOf(TABLENAME_PREFIX) + TABLENAME_PREFIX.length() + 1);
            int position = tempIndex.lastIndexOf(String.format("-%s", TABLENAME_POSTFIX));
            return tempIndex.substring(0, position);
        } else {
            return null;
        }
    }
}
