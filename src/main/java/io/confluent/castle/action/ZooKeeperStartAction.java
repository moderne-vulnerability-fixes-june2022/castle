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

package io.confluent.castle.action;

import io.confluent.castle.cluster.CastleCluster;
import io.confluent.castle.cluster.CastleNode;
import io.confluent.castle.common.CastleUtil;
import io.confluent.castle.role.ZooKeeperRole;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static io.confluent.castle.action.ActionPaths.ZK_CONF;
import static io.confluent.castle.action.ActionPaths.ZK_LOGS;
import static io.confluent.castle.action.ActionPaths.ZK_OPLOGS;
import static io.confluent.castle.action.ActionPaths.ZK_ROOT;

public class ZooKeeperStartAction extends Action  {
    public final static String TYPE = "zooKeeperStart";

    public ZooKeeperStartAction(String scope, ZooKeeperRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[]{
                // We need all nodes to be brought up before we can run this, so that
                // we have access to all node internal hostnames.
                new TargetId(InitAction.TYPE)
            },
            new String[] {},
            role.initialDelayMs());
    }

    @Override
    public void call(final CastleCluster cluster, final CastleNode node) throws Throwable {
        File configFile = null, log4jFile = null, myidFile = null;
        try {
            configFile = writeZooKeeperConfig(cluster, node);
            log4jFile = writeZooKeeperLog4j(cluster, node);
            myidFile = writeMyID(cluster, node);
            CastleUtil.killJavaProcess(cluster, node, ZooKeeperRole.ZOOKEEPER_CLASS_NAME, false);
            node.uplink().command().args(createSetupPathsCommandLine()).mustRun();
            node.uplink().command().syncTo(configFile.getAbsolutePath(),
                ActionPaths.ZK_PROPERTIES).mustRun();
            node.uplink().command().syncTo(log4jFile.getAbsolutePath(),
                ActionPaths.ZK_LOG4J).mustRun();
            node.uplink().command().syncTo(myidFile.getAbsolutePath(),
                ActionPaths.ZK_MYID).mustRun();
            node.uplink().command().args(createRunDaemonCommandLine()).mustRun();
        } finally {
            CastleUtil.deleteFileOrLog(node.log(), configFile);
            CastleUtil.deleteFileOrLog(node.log(), log4jFile);
            CastleUtil.deleteFileOrLog(node.log(), myidFile);
        }
        CastleUtil.waitFor(5, 30000, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return 0 == node.uplink().command().args(
                    CastleUtil.checkJavaProcessStatusArgs(ZooKeeperRole.ZOOKEEPER_CLASS_NAME)).run();
            }
        });
    }

    public static String[] createSetupPathsCommandLine() {
        return new String[]{"-n", "--",
            "sudo", "rm", "-rf", ZK_OPLOGS, ZK_LOGS, ZK_CONF, "&&",
            "sudo", "mkdir", "-p", ZK_OPLOGS, ZK_LOGS, ZK_CONF, "&&",
            "sudo", "chown", "`whoami`", ZK_ROOT, ZK_OPLOGS, ZK_LOGS, ZK_CONF
        };
    }

    public static String[] createRunDaemonCommandLine() {
        return new String[] {"nohup", "env",
            "JMX_PORT=8989",
            "LOG_DIR=\"" + ZK_LOGS + "\"",
            "KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:" + ActionPaths.ZK_LOG4J + "\"",
            ActionPaths.ZK_START_SCRIPT, ActionPaths.ZK_PROPERTIES,
            ">" + ActionPaths.ZK_LOGS + "/stdout-stderr.txt", "2>&1", "</dev/null", "&"};
    }

    private File writeZooKeeperConfig(CastleCluster cluster, CastleNode node) throws IOException {
        ZooKeeperRole role = node.getRole(ZooKeeperRole.class);
        if (role == null) {
            throw new RuntimeException("Expected the ZK node to have the ZooKeeperRole");
        }
        File file = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        boolean success = false;
        try {
            file = new File(cluster.env().workingDirectory(),
                String.format("zookeeper-%d.properties", node.nodeIndex()));
            fos = new FileOutputStream(file, false);
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            if (role.getTickTimeMs() != 0) {
                osw.write(String.format("tickTime=%d%n", role.getTickTimeMs()));
            }
            if (role.getInitLimit() != 0) {
                osw.write(String.format("initLimit=%d%n", role.getInitLimit()));
            }
            if (role.getSyncLimit() != 0) {
                osw.write(String.format("syncLimit=%d%n", role.getSyncLimit()));
            }
            osw.write(String.format("dataDir=%s%n", ZK_OPLOGS));
            osw.write(String.format("clientPort=2181%n"));
            osw.write(String.format("maxClientCnxns=0%n"));
            for (String nodeName : cluster.nodesWithRole(ZooKeeperRole.class).values()) {
                int serverIdx = getServerIdx(cluster, nodeName);
                osw.write(String.format("server.%d=%s:2888:3888%n", serverIdx,
                    cluster.nodes().get(nodeName).uplink().internalDns()));
            }
            success = true;
            return file;
        } finally {
            CastleUtil.closeQuietly(cluster.clusterLog(),
                osw, "temporary ZooKeeper config file OutputStreamWriter");
            CastleUtil.closeQuietly(cluster.clusterLog(),
                fos, "temporary ZooKeeper config file FileOutputStream");
            if (!success) {
                CastleUtil.deleteFileOrLog(node.log(), file);
            }
        }
    }

    private int getServerIdx(CastleCluster cluster, String nodeName) {
        int serverIdx = 1;
        List<String> sortedZkNodeNames = cluster.nodesWithRole(ZooKeeperRole.class).values()
                .stream()
                .sorted()
                .collect(Collectors.toList());
        for (String zkNodeName : sortedZkNodeNames) {
            if(zkNodeName.equals(nodeName)) {
                return serverIdx;
            }
            serverIdx++;
        }
        throw new IllegalStateException("Did not find ZK node with name " + nodeName + " in cluster");
    }

    private File writeMyID(CastleCluster cluster, CastleNode node) throws IOException {
        File file = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        boolean success = false;
        try {
            int serverIdx = getServerIdx(cluster, node.nodeName());
            file = new File(cluster.env().workingDirectory(), String.format("tmp-myid-%d", serverIdx));
            fos = new FileOutputStream(file, false);
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            osw.write(String.format("%d", serverIdx));
            success = true;
            return file;
        } finally {
            CastleUtil.closeQuietly(cluster.clusterLog(),
                    osw, "temporary myid file OutputStreamWriter");
            CastleUtil.closeQuietly(cluster.clusterLog(),
                    fos, "temporary myid file FileOutputStream");
            if (!success) {
                CastleUtil.deleteFileOrLog(node.log(), file);
            }
        }
    }

    static File writeZooKeeperLog4j(CastleCluster cluster, CastleNode node) throws IOException {
        File file = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        boolean success = false;
        try {
            file = new File(cluster.env().workingDirectory(),
                String.format("zookeeper-log4j-%d.properties", node.nodeIndex()));
            fos = new FileOutputStream(file, false);
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            osw.write(String.format("log4j.rootLogger=INFO, kafkaAppender%n"));
            osw.write(String.format("log4j.appender.kafkaAppender=org.apache.log4j.DailyRollingFileAppender%n"));
            osw.write(String.format("log4j.appender.kafkaAppender.DatePattern='.'yyyy-MM-dd-HH%n"));
            osw.write(String.format("log4j.appender.kafkaAppender.File=%s/server.log%n", ZK_LOGS));
            osw.write(String.format("log4j.appender.kafkaAppender.layout=org.apache.log4j.PatternLayout%n"));
            osw.write(String.format("log4j.appender.kafkaAppender.layout.ConversionPattern=%s%n%n",
                "[%d] %p %m (%c)%n"));
            osw.write(String.format("log4j.logger.org.I0Itec.zkclient.ZkClient=INFO%n"));
            osw.write(String.format("log4j.logger.org.apache.zookeeper=INFO%n"));
            success = true;
            return file;
        } finally {
            CastleUtil.closeQuietly(cluster.clusterLog(),
                osw, "temporary broker file OutputStreamWriter");
            CastleUtil.closeQuietly(cluster.clusterLog(),
                fos, "temporary broker file FileOutputStream");
            if (!success) {
                CastleUtil.deleteFileOrLog(node.log(), file);
            }
        }
    }
};
