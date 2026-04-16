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

import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class AsyncExecutorTest
{
    private final NameResolverAgent nameResolver = mock(NameResolverAgent.class);
    private final OneToOneConcurrentArrayQueue<Runnable> queue =
        new OneToOneConcurrentArrayQueue<>(5);
    private final AsyncExecutor executor = new AsyncExecutor(nameResolver, queue);

    @Test
    void shouldCallDoWorkOnNameResolver()
    {
        when(nameResolver.doWork()).thenReturn(0, 1, 3, 0);

        assertEquals(0, executor.doWork());
        assertEquals(1, executor.doWork());
        assertEquals(3, executor.doWork());
        assertEquals(0, executor.doWork());
        assertEquals(0, executor.doWork());

        verify(nameResolver, times(5)).doWork();
    }

    @Test
    void shouldCallDoWorkOnNameResolverAndProcessQueuedCommands()
    {
        when(nameResolver.doWork()).thenReturn(1, 0, 2, 5);

        final Runnable task1 = mock(Runnable.class);
        final Runnable task2 = mock(Runnable.class);
        final Runnable task3 = mock(Runnable.class);
        queue.add(task1);
        queue.add(task2);
        queue.add(task3);

        assertEquals(2, executor.doWork());
        assertEquals(1, executor.doWork());
        assertEquals(3, executor.doWork());
        assertEquals(5, executor.doWork());

        final InOrder inOrder = inOrder(nameResolver, task1, task2, task3);
        inOrder.verify(nameResolver).doWork();
        inOrder.verify(task1).run();
        inOrder.verify(nameResolver).doWork();
        inOrder.verify(task2).run();
        inOrder.verify(nameResolver).doWork();
        inOrder.verify(task3).run();
        inOrder.verify(nameResolver).doWork();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldCallOnStartOnNameResolver()
    {
        executor.onStart();

        verify(nameResolver).onStart();
        verifyNoMoreInteractions(nameResolver);
    }

    @Test
    void shouldCallOnCloseOnNameResolver()
    {
        executor.onClose();

        verify(nameResolver).onClose();
        verifyNoMoreInteractions(nameResolver);
    }

    @Test
    void roleName()
    {
        assertEquals("aeron-executor", executor.roleName());
    }
}
