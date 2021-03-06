package com.hyd.ssdb.conf;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.Date;

/**
 * 对一台具体的 SSDB 服务器的配置，包括地址、端口、校验密码和其他性能配置。
 * created at 15-12-3
 *
 * @author Yiding
 */
public class Server {

    private String host;            // 服务器地址

    private int port;               // 服务器端口

    private String pass;            // 服务器校验密码（可选）

    private boolean master = true;  // 是否是主服务器。

    private int refreshInterval = 30000;

    private GenericObjectPoolConfig poolConfig = createDefaultPoolConfig();     // 连接池配置参数

    private SocketConfig socketConfig = new SocketConfig();     // 网络配置参数

    private Date lastUpdateTime;

    public Server() {
    }

    public Server(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Server(String host, int port, String pass) {
        this.host = host;
        this.port = port;
        this.pass = pass;
    }

    public Server(String host, int port, int timeoutSeconds) {
        this.host = host;
        this.port = port;
        this.socketConfig.setSoTimeout(timeoutSeconds * 1000);
    }

    public Server(String host, int port, int timeoutSeconds, int bufferSize) {
        this.host = host;
        this.port = port;
        this.socketConfig.setSoTimeout(timeoutSeconds * 1000);
        this.socketConfig.setSoBufferSize(bufferSize);
    }

    public Server(String host, int port, String pass, boolean master) {
        this.host = host;
        this.port = port;
        this.pass = pass;
        this.master = master;
    }

    public Server(String host, int port, String pass, boolean master, int soTimeout, int poolMaxTotal) {
        this.host = host;
        this.port = port;
        this.pass = pass;
        this.master = master;
        this.socketConfig.setSoTimeout(soTimeout);
        this.poolConfig.setMaxTotal(poolMaxTotal);
    }

    public Server(String host, int port, String pass, boolean master, SocketConfig socketConfig) {
        this.host = host;
        this.port = port;
        this.pass = pass;
        this.master = master;

        if (socketConfig != null) {
            this.socketConfig = socketConfig;
        }
    }

    public Server(String host, int port, String pass, boolean master,
                  SocketConfig socketConfig, GenericObjectPoolConfig poolConfig) {

        this.host = host;
        this.port = port;
        this.pass = pass;
        this.master = master;

        if (socketConfig != null) {
            this.socketConfig = socketConfig;
        }

        if (poolConfig != null) {
            this.poolConfig = poolConfig;
        }
    }


    private GenericObjectPoolConfig createDefaultPoolConfig() {
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxIdle(1);
        return config;
    }

    public GenericObjectPoolConfig getPoolConfig() {
        return poolConfig;
    }

    public void setPoolConfig(GenericObjectPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
    }

    public SocketConfig getSocketConfig() {
        return socketConfig;
    }

    public void setSocketConfig(SocketConfig socketConfig) {
        this.socketConfig = socketConfig;
    }

    public boolean isMaster() {
        return master;
    }

    public void setMaster(boolean master) {
        this.master = master;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public Server setLastUpdateTime(Date lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
        return this;
    }

    public int getRefreshInterval() {
        return refreshInterval;
    }

    public Server setRefreshInterval(int refreshInterval) {
        this.refreshInterval = refreshInterval;
        return this;
    }

    @SuppressWarnings("SimplifiableIfStatement")
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Server)) return false;

        Server server = (Server) o;

        if (getPort() != server.getPort()) return false;
        return getHost().equals(server.getHost());

    }

    @Override
    public int hashCode() {
        int result = getHost().hashCode();
        result = 31 * result + getPort();
        return result;
    }

    @Override
    public String toString() {
        return "Server{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", master=" + master +
                '}';
    }
}
