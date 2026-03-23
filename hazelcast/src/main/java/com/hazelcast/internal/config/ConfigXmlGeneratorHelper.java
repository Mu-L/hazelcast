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

package com.hazelcast.internal.config;

import com.hazelcast.config.ClassFilter;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.ConfigXmlGenerator.XmlGenerator;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.internal.util.TriTuple;
import com.hazelcast.nio.serialization.compact.CompactSerializer;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hazelcast.config.CompactSerializationConfigAccessor.getCompactSerializableClassNames;
import static com.hazelcast.config.CompactSerializationConfigAccessor.getRegistrations;
import static com.hazelcast.config.CompactSerializationConfigAccessor.getSerializerClassNames;

/**
 * Contains utility methods to be used in the client and member XML config
 * generators.
 */
public final class ConfigXmlGeneratorHelper {

    private ConfigXmlGeneratorHelper() {
    }

    public static void appendClassFilter(XmlGenerator gen, String name, JavaSerializationFilterConfig filter) {
        if (filter != null) {
            gen.open(name, "defaults-disabled", filter.isDefaultsDisabled());
            appendFilterList(gen, "blacklist", ConfigAccessor.getBlacklistOrNull(filter));
            appendFilterList(gen, "whitelist", ConfigAccessor.getWhitelistOrNull(filter));
            gen.close();
        }
    }

    private static void appendFilterList(XmlGenerator gen, String listName, ClassFilter classFilterList) {
        if (classFilterList == null || classFilterList.isEmpty()) {
            return;
        }
        gen.open(listName);
        for (String className : classFilterList.getClasses()) {
            gen.node("class", className);
        }
        for (String packageName : classFilterList.getPackages()) {
            gen.node("package", packageName);
        }
        for (String prefix : classFilterList.getPrefixes()) {
            gen.node("prefix", prefix);
        }
        gen.close();
    }

    /**
     * Generates the Compact serialization configuration out of the given
     * configuration.
     */
    public static void compactSerialization(XmlGenerator gen, CompactSerializationConfig config) {
        gen.open("compact-serialization");
        Map<String, TriTuple<Class, String, CompactSerializer>> registrations = getRegistrations(config);

        List<String> programmaticSerializerClassNames
                = getProgrammaticCompactSerializerClassNames(registrations);
        List<String> programmaticCompactSerializableClassNames
                = getProgrammaticCompactSerializableClassNames(registrations);

        List<String> serializerClassNames = getSerializerClassNames(config);
        List<String> compactSerializableClassNames = getCompactSerializableClassNames(config);

        if (!isNullOrEmpty(serializerClassNames)
                || ! isNullOrEmpty(programmaticSerializerClassNames)) {
            gen.open("serializers");
            appendCompactSerializerClassNames(gen, serializerClassNames);
            appendCompactSerializerClassNames(gen, programmaticSerializerClassNames);

            // close serializers
            gen.close();
        }

        if (!isNullOrEmpty(compactSerializableClassNames)
                || !isNullOrEmpty(programmaticCompactSerializableClassNames)) {
            gen.open("classes");
            appendCompactSerializableClassNames(gen, compactSerializableClassNames);
            appendCompactSerializableClassNames(gen, programmaticCompactSerializableClassNames);

            // close serializers
            gen.close();
        }

        appendClassFilter(gen, "zero-config-filter", config.getZeroConfigFilter());

        // close compact-serialization
        gen.close();
    }

    private static void appendCompactSerializerClassNames(XmlGenerator gen, List<String> classNames) {
        if (isNullOrEmpty(classNames)) {
            return;
        }

        classNames.forEach(className -> gen.node("serializer", className));
    }

    private static void appendCompactSerializableClassNames(XmlGenerator gen, List<String> classNames) {
        if (isNullOrEmpty(classNames)) {
            return;
        }

        classNames.forEach(className -> gen.node("class", className));
    }

    private static List<String> getProgrammaticCompactSerializerClassNames(
            Map<String, TriTuple<Class, String, CompactSerializer>> registrations) {
        if (MapUtil.isNullOrEmpty(registrations)) {
            return null;
        }

        return registrations.values()
                .stream()
                // element3 -> serializer
                .filter(registration -> registration.element3 != null)
                .map(registration -> registration.element3.getClass().getName())
                .collect(Collectors.toList());
    }

    private static List<String> getProgrammaticCompactSerializableClassNames(
            Map<String, TriTuple<Class, String, CompactSerializer>> registrations) {
        if (MapUtil.isNullOrEmpty(registrations)) {
            return null;
        }

        return registrations.values()
                .stream()
                .filter(registration -> registration.element3 == null)
                // element1 -> class name
                .map(registration -> registration.element1.getName())
                .collect(Collectors.toList());
    }

    private static boolean isNullOrEmpty(List<?> list) {
        return list == null || list.isEmpty();
    }

}
