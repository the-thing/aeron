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

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.concurrent.TimeUnit;

import static io.aeron.driver.status.SystemCounterDescriptor.CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED;
import static io.aeron.driver.status.SystemCounterDescriptor.CONDUCTOR_MAX_CYCLE_TIME;
import static io.aeron.driver.status.SystemCounterDescriptor.NAME_RESOLVER_MAX_TIME;
import static io.aeron.driver.status.SystemCounterDescriptor.NAME_RESOLVER_TIME_THRESHOLD_EXCEEDED;
import static io.aeron.driver.status.SystemCounterDescriptor.RECEIVER_CYCLE_TIME_THRESHOLD_EXCEEDED;
import static io.aeron.driver.status.SystemCounterDescriptor.RECEIVER_MAX_CYCLE_TIME;
import static io.aeron.driver.status.SystemCounterDescriptor.SENDER_CYCLE_TIME_THRESHOLD_EXCEEDED;
import static io.aeron.driver.status.SystemCounterDescriptor.SENDER_MAX_CYCLE_TIME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

public class DutyCycleLabelFormatTest
{
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;

    @AfterEach
    void after()
    {
        CloseHelper.close(driver);
    }

    @ParameterizedTest
    @EnumSource(ThreadingMode.class)
    void test(final ThreadingMode threadingMode)
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .threadingMode(threadingMode)
            .conductorCycleThresholdNs(TimeUnit.HOURS.toNanos(2))
            .senderCycleThresholdNs(TimeUnit.MICROSECONDS.toNanos(321))
            .receiverCycleThresholdNs(TimeUnit.MILLISECONDS.toNanos(250))
            .nameResolverThresholdNs(TimeUnit.SECONDS.toNanos(15));

        driver = TestMediaDriver.launch(context, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driver.context().aeronDirectory());

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName())))
        {
            final CountersReader countersReader = aeron.countersReader();
            assertThat(countersReader.getCounterLabel(CONDUCTOR_MAX_CYCLE_TIME.id()), endsWith(": " + threadingMode));
            assertThat(
                countersReader.getCounterLabel(CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED.id()),
                endsWith(": threshold=7200s " + threadingMode));

            assertThat(countersReader.getCounterLabel(SENDER_MAX_CYCLE_TIME.id()), endsWith(": " + threadingMode));
            assertThat(
                countersReader.getCounterLabel(SENDER_CYCLE_TIME_THRESHOLD_EXCEEDED.id()),
                endsWith(": threshold=321us " + threadingMode));

            assertThat(countersReader.getCounterLabel(RECEIVER_MAX_CYCLE_TIME.id()), endsWith(": " + threadingMode));
            assertThat(
                countersReader.getCounterLabel(RECEIVER_CYCLE_TIME_THRESHOLD_EXCEEDED.id()),
                endsWith(": threshold=250ms " + threadingMode));

            assertThat(countersReader.getCounterLabel(NAME_RESOLVER_MAX_TIME.id()), is(NAME_RESOLVER_MAX_TIME.label()));
            assertThat(
                countersReader.getCounterLabel(NAME_RESOLVER_TIME_THRESHOLD_EXCEEDED.id()),
                endsWith(": threshold=15s"));
        }
    }
}
