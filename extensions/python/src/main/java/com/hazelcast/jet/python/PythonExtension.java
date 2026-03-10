/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.python;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.StreamStage;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import javax.annotation.Nonnull;

/**
 * Extension which allows the user to call Python user-defined functions
 * from inside a Jet pipeline.
 *
 * @see PythonTransforms
 * @since 5.7
 */
public interface PythonExtension extends
        StreamStage.StageExtension<String, PythonExtension.PythonStage<StreamStage<String>>>,
        BatchStage.StageExtension<String, PythonExtension.PythonStage<BatchStage<String>>> {

    /**
     * Extends Pipeline stage with ability to call Python user-defined functions
     * @return extension implementation
     */
    @Nonnull
    static PythonExtension python() {
        return new PythonExtensionImpl();
    }

    /**
     * Pipeline stage with ability to call Python user-defined functions.
     *
     * @param <S> Type of the stage which is extended with {@link PythonExtension}
     * @since 5.7
     */
    interface PythonStage<S extends GeneralStage<String>> {

        /**
         * Adds a "map using Python" pipeline stage.
         * See {@link com.hazelcast.jet.python.PythonServiceConfig} for more details.
         */
        @Nonnull
        S map(@Nonnull PythonServiceConfig cfg);

        /**
         * Adds a "map using Python" pipeline stage which uses
         * {@link PythonServiceConfig} configured using fluent builder API in this stage.
         *
         * @see #maxBatchSize(int)
         * @see #baseDir(String)
         * @see #handlerFile(String)
         * @see #handlerModule(String)
         * @see #handlerFunction(String)
         * @see #channelFn(BiFunctionEx)
         */
        @Nonnull
        S map();

        /**
         * Sets the maximum size of a batch for a single request to Python handler.
         *
         * @param maxBatchSize the maximum size of a batch for a single request
         */
        @Nonnull
        PythonStage<S> maxBatchSize(int maxBatchSize);

        /**
         * Sets the base directory where the Python files reside. When you set this,
         * also set the name of the {@link #handlerModule handler module} to
         * identify the location of the handler function (named {@code
         * transform_list()} by convention).
         * <p>
         * If all you need to deploy to Jet is in a single file, you can call {@link
         * #handlerFile} instead.
         */
        @Nonnull
        PythonStage<S> baseDir(@Nonnull String baseDir);

        /**
         * Sets the Python handler file. It must contain the {@linkplain
         * #handlerFunction handler function}. If your Python work is in more
         * than one file, call {@link #baseDir} instead.
         */
        @Nonnull
        PythonStage<S> handlerFile(@Nonnull String handlerFile);

        /**
         * Sets the name of the Python module that has the function that
         * transforms Jet pipeline data.
         */
        @Nonnull
        PythonStage<S> handlerModule(@Nonnull String handlerModule);

        /**
         * Overrides the default name of the Python function that transforms Jet
         * pipeline data. The default name is {@value PythonServiceConfig#HANDLER_FUNCTION_DEFAULT}.
         * It must be defined in the module you configured with {@link
         * #handlerModule}, must take a single argument that is a list of
         * strings, and return another list of strings which has the results of
         * transforming each item in the input list. There must be a strict
         * one-to-one match between the input and output lists.
         */
        @Nonnull
        PythonStage<S> handlerFunction(@Nonnull String handlerFunction);

        /**
         * Sets the channel function. The function receives a host+port tuple, and
         * it's supposed to return a configured instance of {@link
         * ManagedChannelBuilder}. You can use this to configure the channel, for
         * example to configure the maximum message size etc.
         * <p>
         * The default value is {@link NettyChannelBuilder#forAddress
         * NettyChannelBuilder::forAddress}.
         */
        @Nonnull
        PythonStage<S> channelFn(@Nonnull BiFunctionEx<String, Integer, ? extends ManagedChannelBuilder<?>> channelFn);
    }
}
