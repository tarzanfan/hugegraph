/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.backend.store.mysql;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.store.AbstractBackendStoreProvider;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.mysql.MysqlStore.MysqlGraphStore;
import com.baidu.hugegraph.backend.store.mysql.MysqlStore.MysqlSchemaStore;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class MysqlStoreProvider extends AbstractBackendStoreProvider {

    private static final Logger LOG = Log.logger(MysqlStore.class);

    protected String database() {
        return this.name().toLowerCase();
    }

    @Override
    public BackendStore loadSchemaStore(String name) {
        LOG.debug("MysqlStoreProvider load SchemaStore '{}'", name);

        this.checkOpened();
        if (!this.stores.containsKey(name)) {
            BackendStore s = new MysqlSchemaStore(this, this.database(), name);
            this.stores.putIfAbsent(name, s);
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        E.checkState(store instanceof MysqlStore.MysqlSchemaStore,
                     "SchemaStore must be an instance of MysqlSchemaStore");
        return store;
    }

    @Override
    public BackendStore loadGraphStore(String name) {
        LOG.debug("MysqlStoreProvider load GraphStore '{}'", name);

        this.checkOpened();
        if (!this.stores.containsKey(name)) {
            BackendStore s = new MysqlStore.MysqlGraphStore(this, this.database(), name);
            this.stores.putIfAbsent(name, s);
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        E.checkState(store instanceof MysqlGraphStore,
                     "GraphStore must be an instance of MysqlGraphStore");
        return store;
    }

    @Override
    public String type() {
        return "mysql";
    }
}