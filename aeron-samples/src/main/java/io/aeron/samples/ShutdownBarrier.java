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
package io.aeron.samples;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An implementation of {@link org.agrona.concurrent.ShutdownSignalBarrier} that is also a {@link AtomicBoolean}.
 */
public final class ShutdownBarrier extends AtomicBoolean implements AutoCloseable
{
    private static final long serialVersionUID = -4654671469794556979L;
    private final transient CountDownLatch startCloseLatch = new CountDownLatch(1);
    private final transient CountDownLatch completeCloseLatch = new CountDownLatch(1);
    private final transient Thread shutdownHook = new Thread(() ->
    {
        doSignal();
        try
        {
            completeCloseLatch.await();
        }
        catch (final InterruptedException ignore)
        {
            Thread.currentThread().interrupt();
        }
    }, "Shutdown Hook");

    /**
     * Create new instance.
     */
    public ShutdownBarrier()
    {
        super(true);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    /**
     * Await process termination or manual cancellation.
     *
     * @throws InterruptedException if thread is interrupted while waiting.
     */
    public void await() throws InterruptedException
    {
        startCloseLatch.await();
    }

    /**
     * Signal and terminate.
     */
    public void close()
    {
        doSignal();
        completeCloseLatch.countDown();
    }

    /**
     * Programmatically signal this barrier.
     */
    public void signal()
    {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
        doSignal();
    }

    private void doSignal()
    {
        startCloseLatch.countDown();
        set(false);
    }
}
