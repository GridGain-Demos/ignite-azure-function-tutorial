/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gridgain.demo.azurefunction.functions;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

/**
 * Azure Functions with HTTP Trigger.
 */
public class PopulationFunction {

    private static final Object monitor = new Object();

    private static volatile IgniteClient thinClient;

    //Change with the address of your Ignite Kubernetes Service
    private static final String CLUSTER_IP = "13.87.164.252";

    /**
     * This function listens at endpoint "/api/HttpExample". Two ways to invoke it using "curl" command in bash:
     * 1. curl -d "HTTP Body" {your host}/api/HttpExample
     * 2. curl "{your host}/api/HttpExample?name=HTTP%20Query"
     */
    @FunctionName("population")
    public HttpResponseMessage run(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");

        // Determining a type of request
        String query = request.getQueryParameters().get("popGreaterThan");
        String paramValue = request.getBody().orElse(query);

        if (paramValue != null)
            //Getting all the cities with population greater or equal to the specified one.
            return getCitiesPopulationGreaterThan(Integer.valueOf(paramValue), request);

        query = request.getQueryParameters().get("avgInCountry");
        paramValue = request.getBody().orElse(query);

        if (paramValue != null)
            //Calculating average population in the country.
            return getAvgPopulationInCountry(paramValue, request);

        return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body(
            "Not enough parameters are passed to complete your request").build();
    }

    private HttpResponseMessage getCitiesPopulationGreaterThan(int population, HttpRequestMessage<Optional<String>> request) {
        SqlFieldsQuery sqlQuery = new SqlFieldsQuery("SELECT city.name, city.population, country.name " +
            "FROM city JOIN country ON city.countrycode = country.code WHERE city.population >= ? " +
            "GROUP BY country.name, city.name, city.population ORDER BY city.population DESC, city.name")
            .setArgs(population);

        List<List<?>> result = getThinClientConnection().query(sqlQuery).getAll();

        HttpResponseMessage.Builder responseBuilder = request.createResponseBuilder(HttpStatus.OK);

        responseBuilder.header("Content-Type", "text/html");

        StringBuilder response = new StringBuilder("<html><body><table>");

        for (List<?> row: result) {
            response.append("<tr>");

            for (Object column: row)
                response.append("<td>").append(column).append("</td>");

            response.append("</tr>");
        }

        response.append("</table></body></html>");

        return responseBuilder.body(response.toString()).build();
    }

    private HttpResponseMessage getAvgPopulationInCountry(String countryCode, HttpRequestMessage<Optional<String>> request) {
        CloseableHttpClient httpClient = HttpClients.createDefault();

        HttpGet httpGet = new HttpGet("http://" + CLUSTER_IP +
            ":8080/ignite?cmd=exe&name=org.gridgain.demo.azurefunction.compute.AvgCalculationTask&p1=" + countryCode);

        CloseableHttpResponse response = null;

        try {
            response = httpClient.execute(httpGet);

            HttpEntity entity1 = response.getEntity();

            StringWriter writer = new StringWriter();
            IOUtils.copy(entity1.getContent(), writer, StandardCharsets.US_ASCII);

            return request.createResponseBuilder(HttpStatus.OK).body(writer.toString()).build();

        } catch (IOException e) {
            e.printStackTrace();
            return request.createResponseBuilder(HttpStatus.BAD_GATEWAY).body(
                "Failed to execute the request: " + e.getMessage()).build();
        }
        finally {
            try {
                if (response != null)
                    response.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private IgniteClient getThinClientConnection() {
        if (thinClient == null) {
            synchronized (monitor) {
                ClientConfiguration cfg = new ClientConfiguration().setAddresses(CLUSTER_IP + ":10800");

                thinClient = Ignition.startClient(cfg);
            }
        }

        return thinClient;
    }
}
