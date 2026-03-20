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

import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Defines a policy for reflective attribute lookups, used to verify the safety of accessing
 * classes, fields, or methods for query attribute extraction. This interface provides
 * mechanisms to check and possibly modify the accessibility of classes, methods, and fields
 * to ensure compliance with the defined lookup policy.
 */
public interface ReflectiveAttributeLookupPolicy {
    /**
     * The default reflective attribute lookup policy used during queries.
     */
    Policy DEFAULT_POLICY = Policy.CONTROLLED_ACCESS;

    /**
     * Defines the property used to configure the reflective attribute lookup policy for queries.
     * The policy determines how attributes are extracted reflectively from objects during queries.
     * <p>
     * The default value for this property is derived from the policy name of the {@code DEFAULT_POLICY}.
     */
    HazelcastProperty REFLECTIVE_ATTRIBUTE_LOOKUP_POLICY
            = new HazelcastProperty("hazelcast.query.reflectiveAttributeLookupPolicy", DEFAULT_POLICY.getPolicyName());

    /**
     * Verifies that the provided {@link Class} is safe to use for attribute extraction in this policy.
     *
     * @param clazz the class to verify
     * @return the verified class, possibly with accessibility modifications
     * @throws ReflectiveAttributeLookupException if the class is not deemed safe
     */
    Class<?> verifyClass(Class<?> clazz) throws ReflectiveAttributeLookupException;

    /**
     * Verifies that the provided {@link Method} is safe to use for attribute extraction in this policy.
     * @param clazz the class of the method
     * @param method the method to verify
     * @return the verified method, possibly with accessibility modifications
     * @throws ReflectiveAttributeLookupException if the method is not deemed safe
     */
    Method verifyMethod(Class<?> clazz, Method method) throws ReflectiveAttributeLookupException;

    /**
     * Verifies that the provided {@link Field} is safe to use for attribute extraction in this policy.
     * @param clazz the class of the field
     * @param field the field to verify
     * @return the verified field, possibly with accessibility modifications
     * @throws ReflectiveAttributeLookupException if the field is not deemed safe
     */
    Field verifyField(Class<?> clazz, Field field) throws ReflectiveAttributeLookupException;

    /**
     * Enum representing different policies for reflective attribute lookups. Each policy specifies
     * a level of access control applied when performing reflective operations on attributes
     * such as fields and methods.
     */
    enum Policy {
        FULL_ACCESS("full-access", () -> FullAccessReflectiveAttributeLookupPolicy.INSTANCE),
        CONTROLLED_ACCESS("controlled-access", () -> DefaultReflectiveAttributeLookupPolicy.INSTANCE),
        PUBLIC_FIELDS_ONLY("public-fields-only", () -> PublicFieldReflectiveAttributeLookupPolicy.INSTANCE),
        NO_ACCESS("no-access", () -> NoAccessReflectiveAttributeLookupPolicy.INSTANCE),
        ;

        private final String policyName;
        private final Supplier<ReflectiveAttributeLookupPolicy> policySupplier;

        Policy(String policyName, Supplier<ReflectiveAttributeLookupPolicy> policySupplier) {
            this.policyName = policyName;
            this.policySupplier = policySupplier;
        }

        public ReflectiveAttributeLookupPolicy getImplementation() {
            return policySupplier.get();
        }

        public String getPolicyName() {
            return policyName;
        }

        public static Policy fromPolicyName(String policyName) {
            for (Policy policy : values()) {
                if (policy.policyName.equalsIgnoreCase(policyName)) {
                    return policy;
                }
            }
            throw new IllegalArgumentException("Unknown reflective lookup policy name: " + policyName
                    + ". Available policies: " + getPolicyNames());
        }

        public static String getPolicyNames() {
            return Arrays.stream(values()).map(Policy::getPolicyName).collect(Collectors.joining(", "));
        }
    }

    /**
     * Looks up the {@link ReflectiveAttributeLookupPolicy#REFLECTIVE_ATTRIBUTE_LOOKUP_POLICY} property from the provided
     * {@link HazelcastProperties} instance and returns the corresponding {@code ReflectiveAttributeLookupPolicy} implementation.
     *
     * @param properties the {@code HazelcastProperties} instance containing the configuration for selecting the policy
     * @return the resolved {@code ReflectiveAttributeLookupPolicy} implementation
     * @throws IllegalArgumentException if the specified policy name is not recognized
     */
    static ReflectiveAttributeLookupPolicy getPolicy(HazelcastProperties properties) {
        if (properties == null) {
            return DEFAULT_POLICY.getImplementation();
        }
        String policyName = properties.getString(REFLECTIVE_ATTRIBUTE_LOOKUP_POLICY);
        return Policy.fromPolicyName(policyName).getImplementation();
    }
}
