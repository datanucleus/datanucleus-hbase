/**********************************************************************
Copyright (c) 2009 Erik Bengtson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
    ...
***********************************************************************/
package org.datanucleus.store.hbase;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.util.Localiser;

/**
 * Implementation of a ConnectionFactory for HBase.
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory
{
    // TODO These are in RDBMS plugin also, so maybe move to Core plugin at some point
    public static final String PROPERTY_CONNECTION_POOL_TIME_BETWEEN_EVICTOR_RUNS_MILLIS = "datanucleus.connectionPool.timeBetweenEvictionRunsMillis";
    public static final String PROPERTY_CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS = "datanucleus.connectionPool.minEvictableIdleTimeMillis";

    private Configuration config;

    private HBaseConnectionPool connectionPool;

    private int poolMinEvictableIdleTimeMillis = 0;

    /**
     * Constructor.
     * @param storeMgr The context
     * @param resourceType Type of resource (tx, nontx)
     */
    public ConnectionFactoryImpl(StoreManager storeMgr, String resourceType)
    {
        super(storeMgr, resourceType);

        // how often should the evictor run
        int poolTimeBetweenEvictionRunsMillis = storeMgr.getIntProperty(PROPERTY_CONNECTION_POOL_TIME_BETWEEN_EVICTOR_RUNS_MILLIS);
        if (poolTimeBetweenEvictionRunsMillis == 0)
        {
            poolTimeBetweenEvictionRunsMillis = 15 * 1000; // default, 15 secs
        }

        // how long may a connection sit idle in the pool before it may be evicted
        poolMinEvictableIdleTimeMillis = storeMgr.getIntProperty(PROPERTY_CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        if (poolMinEvictableIdleTimeMillis == 0)
        {
            poolMinEvictableIdleTimeMillis = 30 * 1000; // default, 30 secs
        }

        connectionPool = new HBaseConnectionPool();
        connectionPool.setTimeBetweenEvictionRunsMillis(poolTimeBetweenEvictionRunsMillis);

        String url = storeMgr.getConnectionURL();
        if (!url.startsWith("hbase"))
        {
            throw new NucleusException(Localiser.msg("HBase.URLInvalid", url));
        }

        // Split the URL into local, or "host:port"
        String hbaseStr = url.substring(5); // Omit the hbase prefix, and any colon
        if (hbaseStr.startsWith(":"))
        {
            hbaseStr = hbaseStr.substring(1);
        }

        config = ((HBaseStoreManager)storeMgr).getHbaseConfig();
        if (hbaseStr.length() > 0)
        {
            // Remote, so specify server
            String serverName = hbaseStr;
            String portName = "60000";
            if (hbaseStr.indexOf(':') > 0)
            {
                serverName = hbaseStr.substring(0, hbaseStr.indexOf(':'));
                portName = hbaseStr.substring(hbaseStr.indexOf(':')+1);
            }
            config.set("hbase.zookeeper.quorum", serverName);
            config.set("hbase.master", serverName + ":" + portName);
        }

// Old code from HBase 0.9.x when we had HTablePool
//        int maxSize = storeMgr.getIntProperty(HBaseStoreManager.PROPERTY_HBASE_TABLE_POOL_MAXSIZE);
//        maxSize = maxSize > 0 ? maxSize : Integer.MAX_VALUE;
    }

    /**
     * Obtain a connection from the Factory.
     * The connection will be enlisted within the transaction associated to the ExecutionContext
     * @param ec the pool that is bound the connection during its lifecycle (or null)
     * @param options Any options for creating the connection
     * @return the {@link org.datanucleus.store.connection.ManagedConnection}
     */
    public ManagedConnection createManagedConnection(ExecutionContext ec, Map options)
    {
        HBaseManagedConnection managedConnection = connectionPool.getPooledConnection();
        if (managedConnection == null) 
        {
            try
            {
                Connection conn = ConnectionFactory.createConnection(config);
                managedConnection = new HBaseManagedConnection(conn);
                managedConnection.setIdleTimeoutMills(poolMinEvictableIdleTimeMillis);
                connectionPool.registerConnection(managedConnection);
            }
            catch (ZooKeeperConnectionException e)
            {
                throw new NucleusDataStoreException("Exception thrown obtaining HBase Connection", e);
            }
            catch (IOException e)
            {
                throw new NucleusDataStoreException("Exception thrown obtaining HBase Connection", e);
            }
        }
        return managedConnection;
    }
}