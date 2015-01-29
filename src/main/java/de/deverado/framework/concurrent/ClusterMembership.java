package de.deverado.framework.concurrent;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterMembership {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterMembership.class);

    private String nodeId;

    private String clusterName;

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        if (this.nodeId != null) {
            Exception exception = new Exception("Exception for stack trace");
            LOG.error("Reassignment of nodeId, ignoring", exception);
            return;
        }
        this.nodeId = nodeId;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        if (this.clusterName != null) {
            Exception exception = new Exception("Exception for stack trace");
            LOG.error("Reassignment of clusterName, ignoring", exception);
            return;
        }
        this.clusterName = clusterName;
    }

}
