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

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkArgument;

final class PythonExtensionImpl implements PythonExtension {

    @Override
    @Nonnull
    public PythonStage<StreamStage<String>> extend(@Nonnull StreamStage<String> streamStage) {
        return new GeneralPythonStage<>(streamStage);
    }

    @Override
    @Nonnull
    public PythonStage<BatchStage<String>> extend(@Nonnull BatchStage<String> batchStage) {
        return new GeneralPythonStage<>(batchStage);
    }

    static class GeneralPythonStage<S extends GeneralStage<String>> implements PythonStage<S> {
        @Nonnull
        private final S stage;

        /**
         * Collects fluently built configuration
         */
        private final PythonServiceConfig pythonServiceConfig = new PythonServiceConfig();
        private int maxBatchSize = PythonTransforms.DEFAULT_MAX_BATCH_SIZE;

        GeneralPythonStage(@Nonnull S stage) {
            this.stage = stage;
        }

        @Nonnull
        @Override
        public S map(@Nonnull PythonServiceConfig cfg) {
            return map(cfg, maxBatchSize);
        }

        @Nonnull
        @Override
        public S map() {
            return map(pythonServiceConfig);
        }

        @SuppressWarnings("unchecked")
        private S map(@Nonnull PythonServiceConfig cfg, int maxBatchSize) {
            checkArgument(maxBatchSize > 0, "maxBatchSize must be > 0");

            // avoid accidental sharing of the configuration
            PythonServiceConfig clonedConfig = new PythonServiceConfig(cfg);

            // the cast is safe here: mapUsingServiceAsyncBatched does not change type of the stage,
            // and we still have String item.
            return (S) stage.mapUsingServiceAsyncBatched(
                    PythonService.factory(clonedConfig), maxBatchSize, PythonService::sendRequest)
                    .setName("mapUsingPython");
        }

        @Nonnull
        @Override
        public PythonStage<S> maxBatchSize(int maxBatchSize) {
            checkArgument(maxBatchSize > 0, "maxBatchSize must be > 0");
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        @Nonnull
        @Override
        public PythonStage<S> baseDir(@Nonnull String baseDir) {
            pythonServiceConfig.setBaseDir(baseDir);
            return this;
        }

        @Nonnull
        @Override
        public PythonStage<S> handlerFile(@Nonnull String handlerFile) {
            pythonServiceConfig.setHandlerFile(handlerFile);
            return this;
        }

        @Nonnull
        @Override
        public PythonStage<S> handlerModule(@Nonnull String handlerModule) {
            pythonServiceConfig.setHandlerModule(handlerModule);
            return this;
        }

        @Nonnull
        @Override
        public PythonStage<S> handlerFunction(@Nonnull String handlerFunction) {
            pythonServiceConfig.setHandlerFunction(handlerFunction);
            return this;
        }

        @Nonnull
        @Override
        public PythonStage<S> channelFn(@Nonnull BiFunctionEx<String, Integer, ? extends ManagedChannelBuilder<?>> channelFn) {
            pythonServiceConfig.setChannelFn(channelFn);
            return this;
        }
    }

}
