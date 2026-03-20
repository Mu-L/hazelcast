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

/**
 * Indicates that a reflective accessor is not allowed by the configured attribute lookup policy.
 */
public class ReflectiveAttributeLookupException extends Exception {

    public ReflectiveAttributeLookupException(String message) {
        // Append useful information for users to select their policy if they see exceptions
        super(message + String.format("%nThe reflective attribute lookup policy can be changed by using the property '%s' to "
                + "define a policy from one of the following: %s",
                ReflectiveAttributeLookupPolicy.REFLECTIVE_ATTRIBUTE_LOOKUP_POLICY.getName(),
                ReflectiveAttributeLookupPolicy.Policy.getPolicyNames()));
    }
}
