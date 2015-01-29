package de.deverado.framework.concurrent;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import de.deverado.framework.core.propertyconfig.PropsConfiguratorPlugin;
import de.deverado.framework.core.propertyconfig.PropsConfiguratorPluginDefaultImpl;
import de.deverado.framework.core.propertyconfig.PropsConfiguratorPluginFuncAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Create with {@link  #create}.
 */
public class ClusterNodesPropsConfiguratorPlugin  {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterNodesPropsConfiguratorPlugin.class);

    public static PropsConfiguratorPlugin create() {
        return new PropsConfiguratorPluginDefaultImpl(new ClusterNodesPropsConfiguratorPlugin());
    }

    private static final Splitter NODE_SPLITTER = Splitter.on(
            CharMatcher.anyOf(" ,;")).omitEmptyStrings();
    private static final Splitter PORT_SPLITTER = Splitter.on(":");


    @PropsConfiguratorPluginFuncAnnotation
    public String nodesWithClass(Properties config, String key, String val,
                                    List<String> params) {
        if (params.size() < 1) {
            throw new RuntimeException("Illegal configuration, key " + key
                    + ": nodesWithClass needs at least 1 parameter");
        }
        String nodes = config.getProperty("puppet_class_"
                + params.get(0).replace('-', '_').replace(':', '_'));
        if (Strings.isNullOrEmpty(nodes)) {
            return "";
        }
        if (params.size() > 1) {
            if ("VPN_FROM_NAME".equalsIgnoreCase(params.get(1))) {
                Iterable<String> nodesAndPorts = NODE_SPLITTER.split(nodes);
                List<String> result = Lists.newArrayList();
                for (String entry : nodesAndPorts) {
                    Iterator<String> nodeAndPort = PORT_SPLITTER.split(entry)
                            .iterator();
                    String name = nodeAndPort.next();
                    String port = null;
                    if (nodeAndPort.hasNext()) {
                        port = nodeAndPort.next();
                    }
                    String ip = ClusterNodes.clusterHostnameToVpnIp(name);
                    if (Strings.isNullOrEmpty(port)) {
                        result.add(ip);
                    } else {
                        result.add(ip + ":" + port);
                    }
                    nodes = Joiner.on(";").join(result);
                }
            } else {
                LOG.warn("Unknown parameter to nodesWithClass: {}",
                        params.get(1));
            }
        }
        return nodes;
    }
}
