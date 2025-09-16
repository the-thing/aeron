/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.cluster;

import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.IpTables;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.TopologyTest;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.stream.IntStream;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@TopologyTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
@EnabledOnOs(OS.LINUX)
class ClusterNetworkPartitionTest
{
    private static final List<String> HOSTNAMES =
        List.of("127.1.0.0", "127.1.1.0", "127.1.2.0", "127.1.3.0", "127.1.4.0");
    private static final int CLUSTER_SIZE = HOSTNAMES.size();
    private static final String CHAIN_NAME = "CLUSTER-TEST";

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private TestCluster cluster = null;

    @BeforeEach
    void setUp()
    {
        IpTables.setupChain(CHAIN_NAME);
    }

    @AfterEach
    void tearDown()
    {
        IpTables.tearDownChain(CHAIN_NAME);
    }

    @Test
    @InterruptAfter(30)
    void shouldStartClusterThenElectNewLeaderAfterPartition()
    {
        cluster = aCluster()
            .withStaticNodes(CLUSTER_SIZE)
            .withCustomAddresses(HOSTNAMES)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode firstLeader = cluster.awaitLeader();

        cluster.connectClient();
        cluster.sendAndAwaitMessages(100);

        final long initialLeaderLogPosition = firstLeader.appendPosition();

        IpTables.makeSymmetricNetworkPartition(
            CHAIN_NAME,
            List.of(HOSTNAMES.get(firstLeader.index())),
            IntStream.range(0, HOSTNAMES.size())
                .filter(i -> i != firstLeader.index())
                .mapToObj(HOSTNAMES::get)
                .toList());

        cluster.sendMessages(50); // will be sent to the old leader
        Tests.await(() -> firstLeader.appendPosition() > initialLeaderLogPosition);

        final TestNode secondLeader = cluster.awaitLeader(firstLeader.index());
        assertNotEquals(firstLeader.index(), secondLeader.index());

        cluster.awaitNodeState(firstLeader, (n) -> n.electionState() == ElectionState.CANVASS);

        IpTables.flushChain(CHAIN_NAME);

        cluster.awaitNodeState(firstLeader, (n) -> n.electionState() == ElectionState.CLOSED);

        cluster.sendAndAwaitMessages(100, 200);
    }
}
