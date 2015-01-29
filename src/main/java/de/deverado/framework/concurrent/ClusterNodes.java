package de.deverado.framework.concurrent;/*
 * Copyright Georg Koester 2012-2015. All rights reserved.
 */

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Cluster nodes are single members of the cluster - services or machines. They have a lifeness status and should be
 * listening to cluster events, machine events and possibly messages to their services. A node id might be a hostname
 * (no domain part allowed!) or a hostname.serviceId. So multiple nodes per machine are possible - each with their own
 * lifeness and cluster membership.
 * They might:
 * <ul>
 *     <li>do task processing for the general processing queue (or other queues)</li>
 *     <li>host a cluster service which has a service id, so that that they also receive messages under the
 *     hostname.serviceId queue address</li>
 *     <li>host a different service which might function with its own address (e.g. a web server listening on a
 *     socket while also being a cluster node which integrates with cluster messaging</li>
 * </ul>
 *
 * Multiple might be running on the same machine - where ids have to be unique. So only one can use the hostname,
 * others must use a hostname.serviceId combination.
 *
 * <p>
 *     Why not allow domain names for the hostnames? The goal was to create a name that would work in URIs as well
 *     as provide named service ids. This excludes the port number and makes it also quite difficult to distinguish
 *     between the host part and the service part via the dot-separator. On the other hand the system I designed for
 *     the cluster VPN uses non-domain hostnames and so domains weren't really important for me. I would love to see
 *     a proposal which improves on this situation. A vector would be to deviate from the default 6 char naming scheme
 *     for hosts to introduce a new naming and distinguish between them.
 * </p>
 */
public class ClusterNodes {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterNodes.class);

    public static Pattern HOSTNAME_HEX = Pattern
            .compile("i?([0-9a-fA-F][0-9a-fA-F])([0-9a-fA-F][0-9a-fA-F])([0-9a-fA-F][0-9a-fA-F])");

    /**
     *
     * @throws IllegalArgumentException if the hostname is not a cluster hostname (cannot be converted to 10.x.x.x ip).
     */
    public static String clusterHostnameToVpnIp(String hostname) {

        String hostnameHex = hostname.replace('v', 'f').trim();
        Matcher matcher = HOSTNAME_HEX.matcher(hostnameHex);
        boolean found = matcher.find() && matcher.start() == 0;
        if (!found) {
            throw new IllegalArgumentException("wrong argument '" + hostname
                    + "', expecting exactly 6 hex chars like af0001 at start.");
        }

        int firstpart = Integer.parseInt(matcher.group(1), 16);
        int secondpart = Integer.parseInt(matcher.group(2), 16);
        int thirdpart = Integer.parseInt(matcher.group(3), 16);

        return "10." + firstpart + "." + secondpart + "." + thirdpart;
    }

    public static boolean isClusterHostname(String hostname) {

        String hostnameHex = hostname.replace('v', 'f').trim();
        Matcher matcher = HOSTNAME_HEX.matcher(hostnameHex);
        return matcher.find() && matcher.start() == 0;
    }

    /**
     * Can be used to identify a server in the cluster (temporarily, nodeId
     * identifies the role and data, this is the address as returned by the OS).
     *
     */
    public static String getHostNameForCluster() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            String message = "Config problem: Local hostname cannot be determined";
            LOG.error(message);
            throw new RuntimeException(message);
        }
    }

    /**
     *
     */
    public static boolean isNodeId(String complexNodeId) {
        String nodeIdOnly = nodeIdOnly(complexNodeId);
        String serviceType = serviceTypeOnly(complexNodeId);

        if (serviceType != null && (!serviceType.equals(""))
                && !isValidServiceType(serviceType)) {
            return false;
        }
        if (isClusterHostname(nodeIdOnly)) {
            return true;
        }
        return false;
    }

    private static final Pattern SERVICE_TYPE_PATTERN = Pattern
            .compile("[^\\w]"); // non-word

    public static boolean isValidServiceType(String serviceTypeCandidate) {
        if (serviceTypeCandidate == null)
            return false;
        return !SERVICE_TYPE_PATTERN.matcher(serviceTypeCandidate).find();
    }

    public static String nodeIdOnly(String complexNodeId) {
        if (complexNodeId == null)
            return "";
        int pointLoc = complexNodeId.indexOf(".");
        if (pointLoc > -1) {
            return complexNodeId.substring(0, pointLoc);
        } else {
            return complexNodeId;
        }
    }

    /**
     *
     * @return "" if no service type is found
     */
    public static String serviceTypeOnly(String complexNodeId) {
        if (complexNodeId == null)
            return "";
        int pointLoc = complexNodeId.indexOf(".");
        if (pointLoc > -1) {
            return complexNodeId
                    .substring(pointLoc + 1, complexNodeId.length());
        } else {
            return "";
        }
    }

    /**
     * Doesn't say that it's a valid service type
     *
     * @see #isValidServiceType(String)
     */
    public static boolean checkHasServiceType(String complexNodeId) {
        if (complexNodeId == null)
            return false;
        int pointLoc = complexNodeId.indexOf(".");
        return complexNodeId.length() > (pointLoc + 1);
    }

    public static String makeNodeId(String nodeIdCachedNoCreation,
                                    String serviceType) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(serviceType),
                "serviceType needed");
        if (checkHasServiceType(nodeIdCachedNoCreation)) {
            nodeIdCachedNoCreation = nodeIdOnly(nodeIdCachedNoCreation);
        }
        if (serviceType != null && !"".equals(serviceType)) {
            return nodeIdCachedNoCreation + "." + serviceType;
        }
        return nodeIdCachedNoCreation;
    }
}
