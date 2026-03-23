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

import com.hazelcast.config.ClassFilter;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.internal.serialization.impl.ExtraReflectiveCompactSerializationRestrictions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ReflectiveClassFilter {

    private static final ClassFilter JDK_LIB_BLOCKLIST = new ClassFilter()
            .addPrefixes("java.", "javax.", "com.sun.", "sun.", "jdk.");

    @Nullable
    private final ClassFilter blockList;
    @Nullable
    private final ClassFilter allowList;

    // We take the union over all configured allowlists and the union over all configured blocklists
    public ReflectiveClassFilter(@Nonnull CompactSerializationConfig config,
                                 @Nonnull ExtraReflectiveCompactSerializationRestrictions extraRestrictions) {

        ClassFilter defaultBlockList = extraRestrictions.defaultBlockList();
        ClassFilter configuredAllowList = null;
        ClassFilter configuredBlockList = null;

        JavaSerializationFilterConfig configuredFilter = config.getZeroConfigFilter();
        if (configuredFilter != null) {
            boolean areDefaultsDisabled = configuredFilter.isDefaultsDisabled();
            defaultBlockList = areDefaultsDisabled ? null : defaultBlockList;

            // Unfortunately the getters in JavaSerializationFilterConfig are not true getters, they initialise the
            // lists to empty ones. So it is not possible to determine whether an empty list was set intentionally or
            // inadvertently, e.g. through reflectively iterating over config calling getters like we do in some of our
            // tests. Our default behaviour is to reset empty allowlist so nothing is broken accidentally. To disable
            // reflective compact serialization the user can set an empty allowlist and the defaultsDisabled flag.
            configuredAllowList = ConfigAccessor.getWhitelistOrNull(configuredFilter);
            if (configuredAllowList != null && configuredAllowList.isEmpty() && !areDefaultsDisabled) {
                configuredAllowList = null;
            }

            // Empty blocklist is equivalent to not being set.
            configuredBlockList = ConfigAccessor.getBlacklistOrNull(configuredFilter);
            if (configuredBlockList != null && configuredBlockList.isEmpty()) {
                configuredBlockList = null;
            }
        }

        blockList = union(JDK_LIB_BLOCKLIST, defaultBlockList, extraRestrictions.propertyBlockList(), configuredBlockList);
        allowList = union(extraRestrictions.propertyAllowList(), configuredAllowList);
    }

    private static ClassFilter union(ClassFilter... filters) {
        ClassFilter result = null;
        for (ClassFilter filter : filters) {
            if (filter != null) {
                result = result == null ? new ClassFilter(filter) : result.add(filter);
            }
        }
        return result;
    }

    /**
     * A class is restricted from direct reflective serialization if it matches any of the following:
     *
     * <ol>
     *     <li>It is not present on any allowlist</li>
     *     <li>It is present on any blocklist</li>
     *     <li>It is a primitive, array or Void</li>
     * </ol>
     *
     * @param clazz The class to test
     * @return Whether the class is restricted
     */
    public boolean isRestricted(@Nonnull Class<?> clazz) {
        // In Java 9+ a null package is returned if the class is primitive, an array or Void
        // none of which are supported here. Primitive and 1D array fields are supported but
        // these are handled when populating the reader/writers. These classes are never directly
        // associated to a reflective serializer.
        return clazz.getPackage() == null || isOnBlockList(clazz) || !isOnAllowList(clazz);
    }

    private boolean isOnAllowList(Class<?> clazz) {
        return allowList == null || allowList.isListed(clazz.getName());
    }

    private boolean isOnBlockList(Class<?> clazz) {
        return blockList != null && blockList.isListed(clazz.getName());
    }
}
