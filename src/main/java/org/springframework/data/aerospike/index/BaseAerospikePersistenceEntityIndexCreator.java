/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.springframework.data.aerospike.index;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.aerospike.mapping.BasicAerospikePersistentEntity;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.context.MappingContextEvent;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Taras Danylchuk
 */
@Slf4j
@RequiredArgsConstructor
public abstract class BaseAerospikePersistenceEntityIndexCreator implements ApplicationListener<MappingContextEvent<?, ?>> , SmartLifecycle {

    private final boolean createIndexesOnStartup;
    private final AerospikeIndexResolver aerospikeIndexResolver;
    private final Set<AerospikeIndexDefinition> initialIndexes = new HashSet<>();
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    @Override
    public void onApplicationEvent(MappingContextEvent<?, ?> event) {
        if (!createIndexesOnStartup) {
            return;
        }

        PersistentEntity<?, ?> entity = event.getPersistentEntity();
        if (!(entity instanceof BasicAerospikePersistentEntity)) {
            return;
        }

        BasicAerospikePersistentEntity<?> persistentEntity = (BasicAerospikePersistentEntity<?>) entity;
        Set<AerospikeIndexDefinition> indexes = aerospikeIndexResolver.detectIndexes(persistentEntity);
        if (!indexes.isEmpty()) {
            if (!initialized.get()) {
                //gh-115: prevent creating indexes on startup phase when aerospike template have not been created yet
                initialIndexes.addAll(indexes);
                return;
            }
            log.debug("Creating {} indexes for entity[{}]...", indexes, entity.getName());
            installIndexes(indexes);
        }
    }

    @Override
    public void start() {
        initialized.set(true);
        installIndexes(initialIndexes);
        initialIndexes.clear();
    }

    protected abstract void installIndexes(Set<AerospikeIndexDefinition> indexes);

    @Override
    public void stop() {
    }

    @Override
    public boolean isRunning() {
        return initialized.get();
    }
}
