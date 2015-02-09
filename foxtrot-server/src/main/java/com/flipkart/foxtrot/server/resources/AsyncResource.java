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

import java.util.Collections;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.core.common.AsyncDataToken;
import com.flipkart.foxtrot.core.common.CacheUtils;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 26/03/14
 * Time: 7:09 PM
 */
@Path("/v1/async")
@Produces(MediaType.APPLICATION_JSON)
public class AsyncResource {
    private static final Logger logger = LoggerFactory.getLogger(AsyncResource.class);

    @GET
    @Path("/{action}/{id}")
    public Response getResponse(@PathParam("action") final String action, @PathParam("id") final String id) {
        return Response.ok(getData(new AsyncDataToken(action, id))).build();
    }

    @POST
    public Response getResponsePost(final AsyncDataToken dataToken) {
        return Response.ok(getData(dataToken)).build();
    }

    private ActionResponse getData(final AsyncDataToken dataToken) {
        try {
            return CacheUtils.getCacheFor(dataToken.getAction()).get(dataToken.getKey());
        } catch (Exception e) {
            logger.error(String.format("Error fetching data for Action %s Key %s", dataToken.getAction(), dataToken.getKey()), e);
            throw new WebApplicationException(Response.serverError()
                    .entity(Collections.singletonMap("error", "Could not save document: " + e.getMessage()))
                    .build());
        }
    }
}
