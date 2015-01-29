package de.deverado.framework.concurrent;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import de.deverado.framework.core.propertyconfig.PropsConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ClusterNodesPropsConfiguratorPluginTest {

    private PropsConfigurator pc;
    private Properties props;

    @Before
    public void setUp() throws Exception {
        pc = new PropsConfigurator(null, false, ClusterNodesPropsConfiguratorPlugin.create());
        props = new Properties();
        props.put("aKey", "aVal");
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void shouldRunNodesWithClassFunc() {
        String nodeList = "de0001.bisbis.de;de0002.bisbis.de";
        props.put("puppet_class_test__cls", nodeList);
        props.put("k", "$nodesWithClass(test::cls)");
        assertEquals(nodeList, pc.loadPropsFromParameterOnly(props).getProperty("k"));

        props.put("k", "$nodesWithClass(test::NOTTHERE)");
        assertEquals("", pc.loadPropsFromParameterOnly(props).getProperty("k"));

        props.put("k", "$nodesWithClass(test::cls, VPN_FROM_NAME)");
        assertEquals("10.222.0.1;10.222.0.2", pc.loadPropsFromParameterOnly(props).getProperty("k"));
    }

    @Test
    public void shouldRunNodesWithClassFuncWithPorts() {
        String nodeList = "de0001.bisbis.de:1231;de0002.bisbis.de:5990";
        props.put("puppet_class_test__cls", nodeList);

        props.put("k", "$nodesWithClass(test::cls)");
        assertEquals(nodeList, pc.loadPropsFromParameterOnly(props).getProperty("k"));

        props.put("k", "$nodesWithClass(test::cls, VPN_FROM_NAME)");
        assertEquals("10.222.0.1:1231;10.222.0.2:5990", pc.loadPropsFromParameterOnly(props).getProperty("k"));
    }

}
