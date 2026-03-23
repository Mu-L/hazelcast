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

package com.hazelcast.config;

/**
 * Configuration for Serialization Filters and Reflection Filters.
 */
public class JavaSerializationFilterConfig {

    private volatile ClassFilter blacklist;
    private volatile ClassFilter whitelist;
    private volatile boolean defaultsDisabled;

    /**
     * Creates an empty Java serialization filter configuration.
     */
    public JavaSerializationFilterConfig() {
    }

    /**
     * Creates a new Java serialization filter configuration by copying the given configuration.
     *
     * @param javaSerializationFilterConfig the configuration to copy from
     */
    public JavaSerializationFilterConfig(JavaSerializationFilterConfig javaSerializationFilterConfig) {
        ClassFilter inputBlacklist = javaSerializationFilterConfig.blacklist;
        blacklist = inputBlacklist == null ? null : new ClassFilter(inputBlacklist);
        ClassFilter inputWhitelist = javaSerializationFilterConfig.whitelist;
        whitelist = inputWhitelist == null ? null : new ClassFilter(inputWhitelist);
        defaultsDisabled = javaSerializationFilterConfig.defaultsDisabled;
    }

    /**
     * Returns the blacklist filter. If not set, it sets and returns an empty filter.
     *
     * @return the blacklist filter
     */
    public ClassFilter getBlacklist() {
        if (blacklist == null) {
            blacklist = new ClassFilter();
        }
        return blacklist;
    }

    ClassFilter getBlacklistOrNull() {
        return blacklist;
    }

    /**
     * Sets the blacklist filter.
     *
     * @param blackList the blacklist filter to set
     * @return this configuration
     */
    public JavaSerializationFilterConfig setBlacklist(ClassFilter blackList) {
        this.blacklist = blackList;
        return this;
    }

    /**
     * Returns the whitelist filter. If not set, it sets and returns an empty filter.
     *
     * @return the whitelist filter
     */
    public ClassFilter getWhitelist() {
        if (whitelist == null) {
            whitelist = new ClassFilter();
        }
        return whitelist;
    }

    ClassFilter getWhitelistOrNull() {
        return whitelist;
    }

    /**
     * Sets the whitelist filter.
     *
     * @param whiteList the whitelist filter to set
     * @return this configuration
     */
    public JavaSerializationFilterConfig setWhitelist(ClassFilter whiteList) {
        this.whitelist = whiteList;
        return this;
    }

    /**
     * Returns whether the default serialization filters are disabled.
     *
     * @return {@code true} if defaults are disabled, {@code false} otherwise
     */
    public boolean isDefaultsDisabled() {
        return defaultsDisabled;
    }

    /**
     * Sets whether the default serialization filters are disabled.
     *
     * @param defaultsDisabled {@code true} to disable defaults, {@code false} otherwise
     * @return this configuration
     */
    public JavaSerializationFilterConfig setDefaultsDisabled(boolean defaultsDisabled) {
        this.defaultsDisabled = defaultsDisabled;
        return this;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((blacklist == null) ? 0 : blacklist.hashCode());
        result = prime * result + ((whitelist == null) ? 0 : whitelist.hashCode());
        result = prime * result + (defaultsDisabled ? 0 : 1);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        JavaSerializationFilterConfig other = (JavaSerializationFilterConfig) obj;
        return ((blacklist == null && other.blacklist == null) || (blacklist != null && blacklist.equals(other.blacklist)))
                && ((whitelist == null && other.whitelist == null) || (whitelist != null && whitelist.equals(other.whitelist)))
                && defaultsDisabled == other.defaultsDisabled;
    }

    @Override
    public String toString() {
        return "JavaSerializationFilterConfig{defaultsDisabled=" + defaultsDisabled + ", blacklist=" + blacklist
                + ", whitelist=" + whitelist + "}";
    }

}
