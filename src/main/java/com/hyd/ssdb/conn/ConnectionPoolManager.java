package com.hyd.ssdb.conn;

import com.hyd.ssdb.SsdbClientException;
import com.hyd.ssdb.SsdbNoClusterAvailableException;
import com.hyd.ssdb.SsdbNoServerAvailableException;
import com.hyd.ssdb.SsdbSocketFailedException;
import com.hyd.ssdb.conf.Cluster;
import com.hyd.ssdb.conf.SPOFStrategy;
import com.hyd.ssdb.conf.Server;
import com.hyd.ssdb.conf.Sharding;
import com.hyd.ssdb.sharding.ConsistentHashSharding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NetworkManager 有两个职责：
 * 1、管理网络的拓扑结构（通过 Sharding 类），决定请求发送到哪个 SSDB 服务器；
 * 2、当请求发送失败时，自动更新失效的服务器列表，并尝试重新发送请求到同一
 * Cluster 的其他服务器，直到没有服务器可用，才抛出异常。
 * <p/>
 * created at 15-12-3
 *
 * @author Yiding
 */
public class ConnectionPoolManager {

    static final Logger LOG = LoggerFactory.getLogger(ConnectionPoolManager.class);

    private Sharding sharding;  // 负载均衡拓扑结构

    private Map<String, ConnectionPool> connectionPoolMap = new ConcurrentHashMap<String, ConnectionPool>();

    public ConnectionPoolManager(Sharding sharding) {
        this.sharding = sharding;
        this.sharding.initClusters();
    }

    public Sharding getSharding() {
        return sharding;
    }

    /**
     * 根据 key 和操作类型获取一个连接。如果 Cluster 的某个服务器无法创建连接，则自动切换
     * 到其他可用的服务器；如果所有的服务器都不可用，则抛出 SsdbNoServerAvailableException
     *
     * @param key   key
     * @param write 是否是写入操作
     * @return 连接和连接池
     */
    public PoolAndConnection getConnection(String key, boolean write)
            throws SsdbNoServerAvailableException {
        boolean retry = false;
        do {
            Cluster cluster = null;
            ConnectionPool connectionPool = null;
            try {
                cluster = sharding.getClusterByKey(key);
                connectionPool = pickServer(cluster, write);
                Connection connection = connectionPool.borrowObject();
                return new PoolAndConnection(connectionPool, connection);

            } catch (SsdbSocketFailedException e) { // 表示连接创建失败
                LOG.error("Connection failed: ", e);
                if (connectionPool != null) {
                    Server server = connectionPool.getConnectionFactory().getServer();
                    // 将服务器标记为不可用，这样下次 do-while 循环就会跳过该服务器
                    reportInvalidConnection(server.getHost(), server.getPort());
                    retry = true;
                }
            } catch (SsdbNoServerAvailableException e) {  // 遇到单点故障，尝试切换 Cluster
                LOG.error("Connection failed: ", e);
                sharding.reportInvalidCluster(cluster);

                retry = true;

                // 包括key策略，不再尝试切换Cluster
                if (sharding instanceof ConsistentHashSharding) {
                    if (((ConsistentHashSharding) sharding).getSpofStrategy() == SPOFStrategy.PreserveKeySpaceStrategy) {
                        throw e;
                    }
                }
            } catch (SsdbNoClusterAvailableException e) {  // 无法再继续尝试切换 Cluster
                LOG.error("Connection failed: ", e);
                throw e;

            } catch (Exception e) {
                throw new SsdbClientException(e);
            }
        } while (retry);

        throw new SsdbClientException("should not be here");
    }

    /**
     * 取一个服务器。如果是写操作，则只取主服务器；否则随机取一个服务器
     *
     * @param cluster 集群配置
     * @param write   是否是写操作
     * @return 取到的服务器。如果取不到，则抛出异常
     */
    private ConnectionPool pickServer(Cluster cluster, boolean write) {
        Server server;
        if (write) {
            server = cluster.getMaster();
        } else {
            server = cluster.getRandomServer();
        }
        return getConnectionPool(server);
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private ConnectionPool getConnectionPool(final Server server) {
        String key = getConnectionPoolMapKey(server.getHost(), server.getPort());
        synchronized (server) {
            if (connectionPoolMap.containsKey(key)) {
                return connectionPoolMap.get(key);
            }

            ConnectionPool connectionPool = createConnectionPool(server);
            connectionPool.setTestOnReturn(true);
            connectionPool.setTestOnBorrow(true);
            connectionPoolMap.put(key, connectionPool);
            return connectionPool;
        }
    }

    private ConnectionPool createConnectionPool(Server server) {
        return new ConnectionPool(server);
    }

    /**
     * 关闭所有连接池以及集群服务器
     */
    public void close() {
        for (Cluster cluster : sharding.getClusters()) {
            try {
                cluster.close();
            } catch (Exception e) {
                LOG.error("Error closing cluster", e);
            }
        }

        for (ConnectionPool pool : connectionPoolMap.values()) {
            try {
                pool.close();
            } catch (Exception e) {
                LOG.error("Error closing connection pool", e);
            }
        }
    }

    public void reportInvalidConnection(Connection connection) {
        String host = connection.getProperty("host");
        Integer port = connection.getProperty("port");
        reportInvalidConnection(host, port);
    }

    public synchronized void reportInvalidConnection(String host, int port) {
        Server toInvalidate = new Server(host, port);

        for (Cluster cluster : sharding.getClusters()) {
            cluster.markInvalid(toInvalidate);
        }
    }

    private String getConnectionPoolMapKey(String host, int port) {
        return host + ":" + port;
    }
}
