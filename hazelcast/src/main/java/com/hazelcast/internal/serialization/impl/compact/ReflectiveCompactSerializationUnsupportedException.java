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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.nio.serialization.HazelcastSerializationException;

public class ReflectiveCompactSerializationUnsupportedException
        extends HazelcastSerializationException {

    public ReflectiveCompactSerializationUnsupportedException(String message) {
        super(message);
    }

    public ReflectiveCompactSerializationUnsupportedException(Class<?> clazz) {
        this(format(clazz) + " cannot be serialized with zero configuration Compact serialization "
                + "because this type can be serialized with another serialization mechanism or it is restricted. "
                + "To override an existing serialization mechanism you can add " + format(clazz) + " to "
                + "CompactSerializationConfig, or write and register an explicit CompactSerializer for it."
                + "To unrestrict it you can configure the filter in CompactSerializationConfig.");
    }

    public ReflectiveCompactSerializationUnsupportedException(Class<?> clazz, Class<?> fieldClazz) {
        this(format(clazz) + " cannot be serialized with zero configuration Compact serialization because it has a field "
                + "of type " + format(fieldClazz) + " which can be serialized with another serialization mechanism or is "
                + "restricted. To override an existing serialization mechanism you can add " + format(fieldClazz) + " to "
                + "CompactSerializationConfig, or write and register an explicit CompactSerializer for it."
                + "To unrestrict it you can configure the filter in CompactSerializationConfig.");
    }

    private static String format(Class<?> clazz) {
        return String.format("'%s'", clazz.getName());
    }
}
