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

package com.hazelcast.query.impl.getters.policy;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * No-op policy that does not impose any restrictions on reflective attribute lookups.
 * This policy is insecure and should be used with caution.
 */
public final class FullAccessReflectiveAttributeLookupPolicy
        implements ReflectiveAttributeLookupPolicy {
    public static final FullAccessReflectiveAttributeLookupPolicy INSTANCE = new FullAccessReflectiveAttributeLookupPolicy();

    private FullAccessReflectiveAttributeLookupPolicy() {
    }

    @Override
    public Class<?> verifyClass(Class<?> clazz)  {
        return clazz;
    }

    @Override
    public Method verifyMethod(Class<?> clazz, Method method) {
        method.setAccessible(true);
        return method;
    }

    @Override
    public Field verifyField(Class<?> clazz, Field field) {
        field.setAccessible(true);
        return field;
    }
}
