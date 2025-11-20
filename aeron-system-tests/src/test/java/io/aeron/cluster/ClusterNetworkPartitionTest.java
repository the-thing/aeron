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

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.IpTables;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.TopologyTest;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.stream.IntStream;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionMessageHeaderDecoder sessionHeaderDecoder = new SessionMessageHeaderDecoder();
    private TestCluster cluster;

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
            .withClusterId(7)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode firstLeader = cluster.awaitLeader();

        cluster.connectClient();
        cluster.sendAndAwaitMessages(100);

        final long initialLeaderLogPosition = firstLeader.appendPosition();

        IpTables.makeSymmetricNetworkPartition(
            CHAIN_NAME,
            List.of(HOSTNAMES.get(firstLeader.memberId())),
            IntStream.range(0, HOSTNAMES.size())
                .filter(i -> i != firstLeader.memberId())
                .mapToObj(HOSTNAMES::get)
                .toList());

        cluster.sendMessages(50); // will be sent to the old leader
        Tests.await(() -> firstLeader.appendPosition() > initialLeaderLogPosition);

        final TestNode interimLeader = cluster.awaitLeaderWithoutElectionTerminationCheck(firstLeader.memberId());
        assertNotEquals(firstLeader.memberId(), interimLeader.memberId());

        cluster.awaitNodeState(firstLeader, (n) -> n.electionState() == ElectionState.CANVASS);

        IpTables.flushChain(CHAIN_NAME);

        final TestNode finalLeader = cluster.awaitLeader(); // ensure no more elections
        assertNotEquals(firstLeader.memberId(), finalLeader.memberId());

        cluster.reconnectClient();
        cluster.sendAndAwaitMessages(100, 200);
    }

    @Test
    @InterruptAfter(30)
    void shouldRestartClusterWithMajorityNodesBeingSlow()
    {
        cluster = aCluster()
            .withStaticNodes(CLUSTER_SIZE)
            .withCustomAddresses(HOSTNAMES)
            .withClusterId(7)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode firstLeader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        final TestNode fastFollower = followers.get(0);
        final TestNode[] slowFollowers = followers.subList(1, followers.size()).toArray(new TestNode[0]);
        final ClusterMember[] clusterMembers =
            ClusterMember.parse(firstLeader.consensusModule().context().clusterMembers());

        cluster.connectClient();
        final int initialMessageCount = 100;
        cluster.sendAndAwaitMessages(initialMessageCount);

        cluster.takeSnapshot(firstLeader);
        cluster.awaitSnapshotCount(1);

        final int messagesAfterSnapshot = 50;
        final int committedMessageCount = initialMessageCount + messagesAfterSnapshot;
        cluster.sendAndAwaitMessages(messagesAfterSnapshot, committedMessageCount);

        final long commitPositionBeforePartition = firstLeader.commitPosition();

        for (final TestNode slowFollower : slowFollowers)
        {
            final ClusterMember clusterMember = clusterMembers[slowFollower.memberId()];
            assertNotNull(clusterMember);
            assertEquals(slowFollower.memberId(), clusterMember.id());

            IpTables.dropUdpTrafficBetweenHosts(CHAIN_NAME,
                HOSTNAMES.get(firstLeader.memberId()),
                "",
                HOSTNAMES.get(slowFollower.memberId()),
                clusterMember.consensusEndpoint().substring(clusterMember.consensusEndpoint().indexOf(':') + 1));

            IpTables.dropUdpTrafficBetweenHosts(CHAIN_NAME,
                HOSTNAMES.get(firstLeader.memberId()),
                "",
                HOSTNAMES.get(slowFollower.memberId()),
                clusterMember.logEndpoint().substring(clusterMember.logEndpoint().indexOf(':') + 1));
        }

        final int messagesReceivedByMinority = 300;
        cluster.sendMessages(messagesReceivedByMinority); // these messages will be only received by 2 out of 5 nodes

        final long leaderAppendPosition =
            awaitLeaderLogRecording(firstLeader, committedMessageCount + messagesReceivedByMinority);

        Tests.await(() -> leaderAppendPosition == fastFollower.appendPosition());

        // ensure message haven't been processed
        assertEquals(commitPositionBeforePartition, firstLeader.commitPosition());
        assertEquals(committedMessageCount, firstLeader.service().messageCount());
        for (final TestNode follower : followers)
        {
            assertEquals(commitPositionBeforePartition, follower.commitPosition());
            assertEquals(committedMessageCount, follower.service().messageCount());
        }

        cluster.terminationsExpected(true);
        cluster.stopAllNodes();

        IpTables.flushChain(CHAIN_NAME); // remove network partition

        cluster.restartAllNodes(false);
        cluster.awaitLeader();
        cluster.reconnectClient();

        final int newMessages = 200;
        cluster.sendMessages(newMessages);
        cluster.awaitResponseMessageCount(newMessages);
        cluster.awaitServicesMessageCount(committedMessageCount + messagesReceivedByMinority + newMessages);
    }

    @Test
    @SuppressWarnings("MethodLength")
    @Disabled
    @InterruptAfter(30)
    void shouldCatchupFollowerAfterBeingStoppedAndNetworkPartition()
    {
        cluster = aCluster()
            .withStaticNodes(CLUSTER_SIZE)
            .withCustomAddresses(HOSTNAMES)
            .withClusterId(5)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode originalLeader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        TestNode recoveringNode = followers.get(0);
        final TestNode[] majority = followers.subList(1, followers.size()).toArray(new TestNode[0]);
        final ClusterMember[] clusterMembers =
            ClusterMember.parse(originalLeader.consensusModule().context().clusterMembers());

        cluster.connectClient();
        cluster.sendAndAwaitMessages(100);

        cluster.takeSnapshot(originalLeader);
        cluster.awaitSnapshotCount(1);

        cluster.sendAndAwaitMessages(50, 150);

        recoveringNode.isTerminationExpected(true);
        cluster.stopNode(recoveringNode);

        final int committedMessages = 350;
        cluster.sendAndAwaitMessages(200, committedMessages);

        final long commitPosition = originalLeader.commitPosition();

        for (final TestNode node : majority)
        {
            final ClusterMember clusterMember = clusterMembers[node.memberId()];
            assertNotNull(clusterMember);
            assertEquals(node.memberId(), clusterMember.id());

            final String consensusPort =
                clusterMember.consensusEndpoint().substring(clusterMember.consensusEndpoint().indexOf(':') + 1);
            final String logPort =
                clusterMember.logEndpoint().substring(clusterMember.logEndpoint().indexOf(':') + 1);

            // Drop traffic from the leader node
            IpTables.dropUdpTrafficBetweenHosts(
                CHAIN_NAME,
                HOSTNAMES.get(originalLeader.memberId()),
                "",
                HOSTNAMES.get(node.memberId()),
                consensusPort);
            IpTables.dropUdpTrafficBetweenHosts(
                CHAIN_NAME, HOSTNAMES.get(originalLeader.memberId()), "", HOSTNAMES.get(node.memberId()), logPort);

            // Drop traffic from the recovering node
            IpTables.dropUdpTrafficBetweenHosts(
                CHAIN_NAME,
                HOSTNAMES.get(recoveringNode.memberId()),
                "",
                HOSTNAMES.get(node.memberId()),
                consensusPort);
            IpTables.dropUdpTrafficBetweenHosts(
                CHAIN_NAME, HOSTNAMES.get(recoveringNode.memberId()), "", HOSTNAMES.get(node.memberId()), logPort);
        }

        final int uncommittedMessages = 333;
        cluster.sendMessages(uncommittedMessages);

        final long uncommittedAppendPosition =
            awaitLeaderLogRecording(originalLeader, committedMessages + uncommittedMessages);

        // restart stopped node to
        recoveringNode = cluster.startStaticNode(recoveringNode.memberId(), false);

        // this will only work if old leader didn't receive unexpected votes or anything
        recoveringNode.awaitElectionState(ElectionState.FOLLOWER_CATCHUP);
        while ((CountersReader.NULL_COUNTER_ID == recoveringNode.logRecordingCounterId()) ||
            recoveringNode.appendPosition() <= uncommittedAppendPosition)
        {
            Tests.yield();
        }

        // FIXME: Prevent recovering follower from finishing the election

        final TestNode majorityLeader = cluster.awaitLeaderWithoutElectionTerminationCheck(originalLeader.memberId());

        IpTables.flushChain(CHAIN_NAME); // remove network partition

        final TestNode actualNewLeader = cluster.awaitLeader();
        assertEquals(majorityLeader.memberId(), actualNewLeader.memberId());
        final long newCommitPosition = majorityLeader.commitPosition();
        assertThat(newCommitPosition, lessThan(uncommittedAppendPosition));

        // ensure uncommitted message haven't been processed
        assertEquals(newCommitPosition, originalLeader.commitPosition());
        assertEquals(committedMessages, originalLeader.service().messageCount());
        for (final TestNode follower : cluster.followers())
        {
            assertEquals(newCommitPosition, follower.commitPosition());
            assertEquals(committedMessages, follower.service().messageCount());
        }

        cluster.reconnectClient();

        final int newMessages = 100;
        cluster.sendMessages(newMessages);
        cluster.awaitResponseMessageCount(newMessages);
        cluster.awaitServicesMessageCount(committedMessages + newMessages);
    }

    private long awaitLeaderLogRecording(final TestNode leader, final int expectedMessageCount)
    {
        final long firstLeaderLogRecordingId =
            RecordingPos.getRecordingId(leader.mediaDriver().counters(), leader.logRecordingCounterId());

        // await leader to record all ingress messages
        try (AeronArchive aeronArchive = AeronArchive.connect(new AeronArchive.Context()
            .clientName("test")
            .aeronDirectoryName(cluster.startClientMediaDriver().aeronDirectoryName())
            .controlRequestChannel(leader.archive().context().controlChannel())
            .controlRequestStreamId(leader.archive().context().controlStreamId())
            .controlResponseChannel("aeron:udp?endpoint=localhost:0")))
        {
            final Aeron aeron = aeronArchive.context().aeron();
            final String replayChannel = "aeron:udp?endpoint=localhost:18181";
            final int replayStreamId = 1111;
            final long replaySubscriptionId = aeronArchive.startReplay(
                firstLeaderLogRecordingId, 0, AeronArchive.REPLAY_ALL_AND_FOLLOW, replayChannel, replayStreamId);
            final int sessionId = (int)replaySubscriptionId;
            final Subscription subscription =
                aeron.addSubscription(ChannelUri.addSessionId(replayChannel, sessionId), replayStreamId);
            Tests.awaitConnected(subscription);

            final Image image = subscription.imageBySessionId(sessionId);
            assertNotNull(image);
            final MutableInteger messageCount = new MutableInteger();
            final FragmentHandler fragmentHandler = (buffer, offset, length, header) ->
            {
                messageHeaderDecoder.wrap(buffer, offset);
                if (MessageHeaderDecoder.SCHEMA_ID == messageHeaderDecoder.schemaId() &&
                    SessionMessageHeaderDecoder.TEMPLATE_ID == messageHeaderDecoder.templateId())
                {
                    sessionHeaderDecoder.wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.blockLength(),
                        messageHeaderDecoder.version());
                    messageCount.increment();
                }
            };

            while (messageCount.get() < expectedMessageCount)
            {
                if (0 == image.poll(fragmentHandler, 100))
                {
                    Tests.yield();
                }
            }

            final long position = image.position();

            subscription.close();
            aeronArchive.stopReplay(replaySubscriptionId);

            return position;
        }
    }
}
