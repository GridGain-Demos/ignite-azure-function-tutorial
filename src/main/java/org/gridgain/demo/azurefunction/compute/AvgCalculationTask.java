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

package org.gridgain.demo.azurefunction.compute;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Compute task that calculate average population across all the cities of a given country. A destination node that
 * stores a primary copy of the partition with the cities is calculated during the map phase. After that, a job is sent
 * for the execution to that only avoiding a broadcast or cluster-wide full-scan operation.
 */
public class AvgCalculationTask implements ComputeTask<String, String> {
    @IgniteInstanceResource
    private Ignite ignite;

    private int partition;

    private ClusterNode node;

    @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable String countryCode) throws IgniteException {

        //Getting a cluster node and a partition that store a primary copy of all the cities with 'countryCode'.
        Affinity<String> affinity = ignite.affinity("Country");

        node = affinity.mapKeyToNode(countryCode);

        partition = affinity.partition(countryCode);

        //Scheduling the task for calculation on that primary node only.
        HashMap<AvgCalculationJob, ClusterNode> executionMap = new HashMap<>();

        executionMap.put(new AvgCalculationJob(countryCode, partition), node);

        return executionMap;
    }

    @Override
    public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        return ComputeJobResultPolicy.WAIT;
    }

    @Nullable @Override public String reduce(List<ComputeJobResult> results) throws IgniteException {
        //Reducing calculation results - only one result is possible
        int[] result = results.get(0).getData();

        if (result == null)
            return "Wrong country code, no cities found";

        return "Calculation Result [avgPopulation=" + result[0] + ", citiesCount=" + result[1] +
            ", partition=" + partition + ", nodeId=" + node.id() + ", nodeAddresses=" + node.addresses() + "]";
    }

    /**
     * Compute job that calculates average population across all the cities of a given country.
     * The job iterates only over a single data partition.
     */
    private static class AvgCalculationJob implements ComputeJob {
        @IgniteInstanceResource
        private Ignite ignite;

        private String countryCode;

        private int partition;

        public AvgCalculationJob(String countryCode, int partition) {
            this.partition = partition;
            this.countryCode = countryCode;
        }

        @Override public Object execute() throws IgniteException {
            //Accessing object records with BinaryObject interface that avoids a need of deserialization and doesn't
            //require to keep models' classes on the server nodes.
            IgniteCache<BinaryObject, BinaryObject> cities = ignite.cache("City").withKeepBinary();

            ScanQuery<BinaryObject, BinaryObject> scanQuery = new ScanQuery<>(partition,
                new IgniteBiPredicate<BinaryObject, BinaryObject>() {
                @Override public boolean apply(BinaryObject key, BinaryObject object) {
                    //Filtering out cities of other countries that stored in the same partition.
                    return key.field("CountryCode").equals(countryCode);
                }
            });

            //Extra hint to Ignite that the data is available locally.
            scanQuery.setLocal(true);

            //Calculation average population across the cities.
            QueryCursor<Cache.Entry<BinaryObject, BinaryObject>> cursor = cities.query(scanQuery);

            long totalPopulation = 0;
            int citiesNumber = 0;

            for (Cache.Entry<BinaryObject, BinaryObject> entry: cursor) {
                totalPopulation += (int)entry.getValue().field("Population");
                citiesNumber++;
            }

            return citiesNumber == 0 ? null : new int[] {(int)(totalPopulation/citiesNumber), citiesNumber};
        }

        @Override public void cancel() {
            System.out.println("Task is cancelled");
        }
    }
}


