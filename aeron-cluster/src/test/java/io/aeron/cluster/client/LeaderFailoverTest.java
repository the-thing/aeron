package io.aeron.cluster.client;

import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(InterruptingTestCallback.class)
class LeaderFailoverTest {

    private TestCluster cluster;

    @BeforeEach
    void setup() {
        cluster = TestCluster.aCluster()
                .withStaticNodes(3)
                .withLeaderHeartbeatTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500L))
                .withServiceSupplier((i) -> new TestNode.TestService[]{new TestNode.MessageTrackingService(1, i)})
                .start();
    }

    @AfterEach
    void teardown() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    @InterruptAfter(15)
    void testClientClusterDownDetection() {
        cluster.awaitLeader();

        List<TestNode> followers = cluster.followers();
        assertEquals(2, followers.size());

        TestNode leader = cluster.findLeader();
        assertNotNull(leader);

        IdleStrategy idleStrategy = new YieldingIdleStrategy();
        idleStrategy.reset();

        AeronCluster.Context clientContext = cluster.clientCtx().
                newLeaderTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500L));

        TestNode.MessageTrackingService service = (TestNode.MessageTrackingService) leader.services()[0];

        UnsafeBuffer message = new UnsafeBuffer();
        message.wrap(ByteBuffer.allocateDirect(64));

        try (AeronCluster client = cluster.connectClient(clientContext)) {
            assertTrue(client.ingressPublication().isConnected());
            assertTrue(client.ingressPublication().availableWindow() > 0);

            leader.close();

            long keepAliveIntervalNs = TimeUnit.MILLISECONDS.toNanos(10L);
            long keepAliveDeadlineNs = System.nanoTime() + keepAliveIntervalNs;

            long sendMessageIntervalNs = TimeUnit.MILLISECONDS.toNanos(20L);
            long sendMessageDeadlineNs = System.nanoTime() + sendMessageIntervalNs;

            long start = System.nanoTime();
            long end;

            int sentMessageCount = 0;

            while (true) {
                long nowNs = System.nanoTime();

                if (client.isClosed()) {
                    end = nowNs;
                    System.out.println("client is closed");
                    break;
                }

                Publication ingressPublication = client.ingressPublication();

                if (ingressPublication == null) {
                    end = nowNs;
                    System.out.println("no ingress publication");
                    break;
                }

                if (!ingressPublication.isConnected()) {
                    end = nowNs;
                    System.out.println("ingress publication is not connected");
                    break;
                }

                if (ingressPublication.isClosed()) {
                    end = nowNs;
                    System.out.println("ingress publication is closed");
                    break;
                }

                if (ingressPublication.availableWindow() <= 0) {
                    end = nowNs;
                    System.out.println("ingress publication available window <= 0");
                    break;
                }

                Subscription egressSubscription = client.egressSubscription();

                if (egressSubscription == null) {
                    end = nowNs;
                    System.out.println("no egress subscription");
                    break;
                }

                if (!egressSubscription.isConnected()) {
                    end = nowNs;
                    System.out.println("egress subscription is not connected");
                    break;
                }

                if (egressSubscription.isClosed()) {
                    end = nowNs;
                    System.out.println("egress subscription is closed");
                    break;
                }

                if (nowNs >= keepAliveDeadlineNs) {
                    boolean success = client.sendKeepAlive();

                    if (!success) {
                        end = nowNs;
                        System.out.println("keep alive send failed");
                        break;
                    }

                    keepAliveDeadlineNs = nowNs + keepAliveIntervalNs;
                }

                if (nowNs >= sendMessageDeadlineNs) {
                    long position = client.offer(message, 0, 8);

                    if (position < 0L) {
                        end = nowNs;
                        System.out.println("offer failed: " + position);
                        break;
                    }

                    sentMessageCount++;
                    sendMessageDeadlineNs = nowNs + sendMessageIntervalNs;
                }

                if (Thread.currentThread().isInterrupted()) {
                    throw new RuntimeException("Interrupted");
                }

                idleStrategy.idle();
            }

            System.out.println("Time (ms): " + TimeUnit.NANOSECONDS.toMillis(end - start));
            System.out.println("Client sent: " + sentMessageCount);
            System.out.println("Cluster received: " + service.clientMessages().size());
        }
    }
}
