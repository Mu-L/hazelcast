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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.ClassFilter;
import com.hazelcast.config.Config;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.List;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class ReflectiveSerializationRestrictionTest
        extends AbstractReflectiveSerializationRestrictionTest {

    @Parameter(1)
    public Boolean isSuccessExpected;

    @Parameter(2)
    public JavaSerializationFilterConfig config;

    @Override
    protected boolean isSuccessExpected() {
        return isSuccessExpected;
    }

    @Override
    protected ClientConfig getClientConfig() {
        ClientConfig result = super.getClientConfig();
        result.getSerializationConfig().getCompactSerializationConfig().setZeroConfigFilter(config);
        return result;
    }

    @Override
    public Config getConfig() {
        Config result = super.getConfig();
        result.getSerializationConfig().getCompactSerializationConfig().setZeroConfigFilter(config);
        return result;
    }

    @Parameters(name = "expectSuccess={1} config={2}")
    public static List<Object[]> parameters() {
        Class<?> clazz = RestrictionTestDto.class;
        String name = clazz.getName();
        String pkg = clazz.getPackageName();
        String prefix = clazz.getPackageName() + ".";
        RestrictionTestDto obj = new RestrictionTestDto(5);
        return List.of(
                new Object[]{obj, true, filter(false, null, null)},
                new Object[]{obj, true, filter(true, null, null)},
                new Object[]{obj, true, filter(false, null, new ClassFilter())},
                new Object[]{obj, false, filter(true, null, new ClassFilter())},
                new Object[]{obj, false, filter(true, new ClassFilter(), new ClassFilter())},
                new Object[]{obj, true, filter(false, new ClassFilter(), new ClassFilter())},

                // Blocklist cases
                new Object[]{obj, false, filter(false, new ClassFilter().addClasses(name), null)},
                new Object[]{obj, false, filter(true, new ClassFilter().addClasses(name), null)},

                new Object[]{obj, false, filter(false, new ClassFilter().addPackages(pkg), null)},
                new Object[]{obj, false, filter(true, new ClassFilter().addPackages(pkg), null)},

                new Object[]{obj, false, filter(false, new ClassFilter().addPrefixes(prefix), null)},
                new Object[]{obj, false, filter(true, new ClassFilter().addPrefixes(prefix), null)},

                // Allowlist cases
                new Object[]{obj, true, filter(false, null, new ClassFilter().addClasses(name))},
                new Object[]{obj, true, filter(true, null, new ClassFilter().addClasses(name))},

                new Object[]{obj, true, filter(false, null, new ClassFilter().addPackages(pkg))},
                new Object[]{obj, true, filter(true, null, new ClassFilter().addPackages(pkg))},

                new Object[]{obj, true, filter(false, null, new ClassFilter().addPrefixes(prefix))},
                new Object[]{obj, true, filter(true, null, new ClassFilter().addPrefixes(prefix))}
        );
    }

    private static JavaSerializationFilterConfig filter(boolean defaultsDisabled, ClassFilter blockList, ClassFilter allowList) {
        return new JavaSerializationFilterConfig()
                .setDefaultsDisabled(defaultsDisabled)
                .setBlacklist(blockList)
                .setWhitelist(allowList);
    }

    public static class RestrictionTestDto  {
        private int n;

        public RestrictionTestDto(int n) {
            this.n = n;
        }

        public int getN() {
            return n;
        }

        @Override
        public String toString() {
            return "RestrictionTestDto{n=" + n + '}';
        }
    }
}
