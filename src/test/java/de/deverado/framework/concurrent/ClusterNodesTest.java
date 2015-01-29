package de.deverado.framework.concurrent;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClusterNodesTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void shouldConvertNameToVpnIp() {
        assertEquals("10.175.0.2", ClusterNodes.clusterHostnameToVpnIp("av0002"));
        // converts v to f
        assertEquals("10.175.0.2", ClusterNodes.clusterHostnameToVpnIp("av0002"));
        assertEquals("10.175.0.2", ClusterNodes.clusterHostnameToVpnIp("iav0002"));

        assertEquals("10.5.255.17", ClusterNodes.clusterHostnameToVpnIp("i05ff11"));
    }

}
