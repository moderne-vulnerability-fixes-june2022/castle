/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.castle.role;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.castle.action.Action;
import io.confluent.castle.action.ZooKeeperStartAction;
import io.confluent.castle.action.ZooKeeperStatusAction;
import io.confluent.castle.action.ZooKeeperStopAction;
import io.confluent.castle.cluster.CastleCluster;
import io.confluent.castle.cluster.CastleNode;
import io.confluent.castle.common.DynamicVariableProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class ZooKeeperRole implements Role {
    public static final String ZOOKEEPER_CLASS_NAME =
        "org.apache.zookeeper.server.quorum.QuorumPeerMain";

    private final int initialDelayMs;
    private final int tickTimeMs;
    private final int initLimit;
    private final int syncLimit;

    @JsonCreator
    public ZooKeeperRole(@JsonProperty("initialDelayMs") int initialDelayMs,
                         @JsonProperty("tickTime") int tickTimeMs,
                         @JsonProperty("initLimit") int initLimit,
                         @JsonProperty("syncLimit") int syncLimit) {
        this.initialDelayMs = initialDelayMs;
        this.tickTimeMs = tickTimeMs;
        this.initLimit = initLimit;
        this.syncLimit = syncLimit;
    }

    @JsonProperty
    public int initialDelayMs() {
        return initialDelayMs;
    }

    @JsonProperty
    public int getTickTimeMs() {
        return tickTimeMs;
    }

    @JsonProperty
    public int getInitLimit() {
        return initLimit;
    }

    @JsonProperty
    public int getSyncLimit() {
        return syncLimit;
    }
    @Override
    public Collection<Action> createActions(String nodeName) {
        ArrayList<Action> actions = new ArrayList<>();
        actions.add(new ZooKeeperStartAction(nodeName, this));
        actions.add(new ZooKeeperStatusAction(nodeName, this));
        actions.add(new ZooKeeperStopAction(nodeName, this));
        return actions;
    }

    @Override
    public Map<String, DynamicVariableProvider> dynamicVariableProviders() {
        return Collections.singletonMap("zkConnect", new DynamicVariableProvider(0) {
            @Override
            public String calculate(CastleCluster cluster, CastleNode node) {
                return cluster.getZooKeeperConnectString();
            }
        });
    }


};
