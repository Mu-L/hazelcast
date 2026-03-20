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
import java.lang.reflect.Modifier;

/**
 * PublicFieldReflectiveAttributeLookupPolicy is an implementation of the
 * {@link ReflectiveAttributeLookupPolicy} interface that only allows public
 * fields to be accessed, and no methods.
 */
public final class PublicFieldReflectiveAttributeLookupPolicy
        implements ReflectiveAttributeLookupPolicy {
    public static final PublicFieldReflectiveAttributeLookupPolicy INSTANCE = new PublicFieldReflectiveAttributeLookupPolicy();

    private final DefaultReflectiveAttributeLookupPolicy verifyClassDelegate;

    private PublicFieldReflectiveAttributeLookupPolicy() {
        this.verifyClassDelegate = DefaultReflectiveAttributeLookupPolicy.INSTANCE;
    }

    @Override
    public Class<?> verifyClass(Class<?> clazz) throws ReflectiveAttributeLookupException {
        // delegate class verification to the controlled access policy
        return verifyClassDelegate.verifyClass(clazz);
    }

    @Override
    public Method verifyMethod(Class<?> clazz, Method method) throws ReflectiveAttributeLookupException {
        throw new ReflectiveAttributeLookupException("The configured reflective attribute lookup policy does not "
                + "support method access");

    }

    @Override
    public Field verifyField(Class<?> clazz, Field field) throws ReflectiveAttributeLookupException {
        if (Modifier.isStatic(field.getModifiers())) {
            throw new ReflectiveAttributeLookupException("Field " + clazz.getName() + "#" + field.getName()
                    + " is static and cannot be used for attribute extraction");
        }
        if (!Modifier.isPublic(field.getModifiers())) {
            throw new ReflectiveAttributeLookupException("Field " + clazz.getName() + "#" + field.getName()
                    + " is not public and cannot be used for attribute extraction with the configured policy");
        }
        if (!Modifier.isPublic(field.getDeclaringClass().getModifiers())) {
            field.setAccessible(true);
        }
        return field;
    }
}
