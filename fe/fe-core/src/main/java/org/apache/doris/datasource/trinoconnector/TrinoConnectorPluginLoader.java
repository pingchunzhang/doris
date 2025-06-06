// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource.trinoconnector;

import org.apache.doris.common.Config;
import org.apache.doris.common.EnvUtils;
import org.apache.doris.trinoconnector.TrinoConnectorPluginManager;

import com.google.common.util.concurrent.MoreExecutors;
import io.trino.FeaturesConfig;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.TypeRegistry;
import io.trino.server.ServerPluginsProvider;
import io.trino.server.ServerPluginsProviderConfig;
import io.trino.spi.type.TypeOperators;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;

// Noninstancetiable utility class
public class TrinoConnectorPluginLoader {
    private static final Logger LOG = LogManager.getLogger(TrinoConnectorPluginLoader.class);

    // Suppress default constructor for noninstantiability
    private TrinoConnectorPluginLoader() {
        throw new AssertionError();
    }

    private static class TrinoConnectorPluginLoad {
        private static FeaturesConfig featuresConfig = new FeaturesConfig();
        private static TypeOperators typeOperators = new TypeOperators();
        private static HandleResolver handleResolver = new HandleResolver();
        private static TypeRegistry typeRegistry;
        private static TrinoConnectorPluginManager trinoConnectorPluginManager;

        static {
            try {
                // Allow self-attachment for Java agents,this is required for certain debugging and monitoring functions
                System.setProperty("jdk.attach.allowAttachSelf", "true");
                // Get the operating system name
                String osName = System.getProperty("os.name").toLowerCase();
                // Skip HotSpot SAAttach for Mac/Darwin systems to avoid potential issues
                if (osName.contains("mac") || osName.contains("darwin")) {
                    System.setProperty("jol.skipHotspotSAAttach", "true");
                }
                // Trino uses jul as its own log system, so the attributes of JUL are configured here
                System.setProperty("java.util.logging.SimpleFormatter.format",
                        "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s: %5$s%6$s%n");
                java.util.logging.Logger logger = java.util.logging.Logger.getLogger("");
                logger.setUseParentHandlers(false);
                FileHandler fileHandler = new FileHandler(EnvUtils.getDorisHome() + "/log/trinoconnector%g.log",
                        500000000, 10, true);
                fileHandler.setLevel(Level.INFO);
                fileHandler.setFormatter(new SimpleFormatter());
                logger.addHandler(fileHandler);
                java.util.logging.LogManager.getLogManager().addLogger(logger);

                typeRegistry = new TypeRegistry(typeOperators, featuresConfig);
                ServerPluginsProviderConfig serverPluginsProviderConfig = new ServerPluginsProviderConfig()
                        .setInstalledPluginsDir(new File(checkAndReturnPluginDir()));
                ServerPluginsProvider serverPluginsProvider = new ServerPluginsProvider(serverPluginsProviderConfig,
                        MoreExecutors.directExecutor());
                trinoConnectorPluginManager = new TrinoConnectorPluginManager(serverPluginsProvider,
                        typeRegistry, handleResolver);
                trinoConnectorPluginManager.loadPlugins();
            } catch (Exception e) {
                LOG.warn("Failed load trino-connector plugins from  " + checkAndReturnPluginDir()
                        + ", Exception:" + e.getMessage(), e);
            }
        }

        private static String checkAndReturnPluginDir() {
            final String defaultDir = System.getenv("DORIS_HOME") + "/plugins/connectors";
            final String defaultOldDir = System.getenv("DORIS_HOME") + "/connectors";
            if (Config.trino_connector_plugin_dir.equals(defaultDir)) {
                // If true, which means user does not set `trino_connector_plugin_dir` and use the default one.
                // Because in 2.1.8, we change the default value of `trino_connector_plugin_dir`
                // from `DORIS_HOME/connectors` to `DORIS_HOME/plugins/connectors`,
                // so we need to check the old default dir for compatibility.
                File oldDir = new File(defaultOldDir);
                if (oldDir.exists() && oldDir.isDirectory()) {
                    String[] contents = oldDir.list();
                    if (contents != null && contents.length > 0) {
                        // there are contents in old dir, use old one
                        return defaultOldDir;
                    }
                }
                return defaultDir;
            } else {
                // Return user specified dir directly.
                return Config.trino_connector_plugin_dir;
            }
        }
    }

    public static FeaturesConfig getFeaturesConfig() {
        return TrinoConnectorPluginLoad.featuresConfig;
    }

    public static TypeOperators getTypeOperators() {
        return TrinoConnectorPluginLoad.typeOperators;
    }

    public static HandleResolver getHandleResolver() {
        return TrinoConnectorPluginLoad.handleResolver;
    }

    public static TypeRegistry getTypeRegistry() {
        return TrinoConnectorPluginLoad.typeRegistry;
    }

    public static TrinoConnectorPluginManager getTrinoConnectorPluginManager() {
        return TrinoConnectorPluginLoad.trinoConnectorPluginManager;
    }
}
