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
package io.aeron.driver;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;

final class AsyncExecutor implements Agent
{
    private final OneToOneConcurrentArrayQueue<Runnable> tasks;
    private final NameResolverAgent nameResolver;

    AsyncExecutor(final NameResolverAgent nameResolver, final OneToOneConcurrentArrayQueue<Runnable> tasks)
    {
        this.tasks = tasks;
        this.nameResolver = nameResolver;
    }

    /**
     * {@inheritDoc}
     */
    public void onStart()
    {
        nameResolver.onStart();
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        int workCount = nameResolver.doWork();

        final Runnable task = tasks.poll();
        if (null != task)
        {
            task.run();
            workCount++;
        }

        return workCount;
    }

    /**
     * {@inheritDoc}
     */
    public void onClose()
    {
        nameResolver.onClose();
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return "aeron-executor";
    }
}
