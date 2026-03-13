/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.merge;

import com.hazelcast.ringbuffer.impl.ArrayRingbufferTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertThrows;

@QuickTest
@ParallelJVMTest
class RingbufferMergeDataReadOnlyIteratorTest {
    /** @see <a href="https://github.com/hazelcast/hazelcast/issues/26570">issue #26570</a> */
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 10})
    void testNextThrowsExceptionWhenEmpty(int capacity) {
        RingbufferMergeDataReadOnlyIterator<String> iterator = new RingbufferMergeDataReadOnlyIterator<>(
                new RingbufferMergeData(ArrayRingbufferTest.fullRingbuffer(capacity)));

        // Consume iterator
        while (iterator.hasNext()) {
            iterator.next();
        }

        assertThrows(NoSuchElementException.class, iterator::next);
    }
}
