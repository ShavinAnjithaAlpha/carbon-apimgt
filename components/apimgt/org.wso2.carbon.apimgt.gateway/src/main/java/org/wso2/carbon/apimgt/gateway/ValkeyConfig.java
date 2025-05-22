package org.wso2.carbon.apimgt.gateway;

import java.util.LinkedList;
import java.util.List;

/**
 * Holds the connection and configuration parameter related to Valkey Cluster which will be used as the distributed counter
 */
public class ValkeyConfig {

    private final List<String> hostAddresses = new LinkedList<>();
    private final List<Integer> ports = new LinkedList<>();
    private boolean isValkeyEnabled;
    private String user;
    private String password;
    private int connectionTimeout;
    private int redirections;

    public void addHostAndPort(String host, int port) {
        hostAddresses.add(host);
        ports.add(port);
    }

    public List<String> getHostAddresses() {
        return hostAddresses;
    }

    public List<Integer> getPorts() {
        return ports;
    }

    public boolean isValkeyEnabled() {
        return isValkeyEnabled;
    }

    public void setValkeyEnabled(boolean valkeyEnabled) {
        isValkeyEnabled = valkeyEnabled;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getRedirections() {
        return redirections;
    }

    public void setRedirections(int redirections) {
        this.redirections = redirections;
    }
}
