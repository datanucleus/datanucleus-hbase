/**********************************************************************
Copyright (c) 2009 Tatsuya Kawano and others. All rights reserved.
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
2012 Andy Jefferson - adapted to not close tables when ref count is back to 0 when in txn
2014 Andy Jefferson - updated to latest HBase API without HTablePool
    ...
***********************************************************************/
package org.datanucleus.store.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.xa.XAResource;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;

/**
 * Implementation of a ManagedConnection.
 */
public class HBaseManagedConnection extends AbstractManagedConnection
{
    private Connection conn;

    /** Cache of HTables used by this connection. */
    private Map<String, Table> tables;

    private int idleTimeoutMills = 30 * 1000; // 30 secs

    private long expirationTime;

    private boolean isDisposed = false;

    public HBaseManagedConnection(Connection conn)
    {
        this.conn = conn;
        this.tables = new HashMap<>();
        disableExpirationTime();
    }

    public Object getConnection()
    {
        return conn;
    }

    public Table getHTable(String tableNameString)
    {
        Table table = tables.get(tableNameString);
        if (table == null)
        {
            try
            {
                table = conn.getTable(TableName.valueOf(tableNameString));
                tables.put(tableNameString, table);
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException("Exception obtaining Table from Connection for table=" + tableNameString, e);
            }
        }
        return table;
    }

    public XAResource getXAResource()
    {
        return null;
    }

    public void close()
    {
        for (ManagedConnectionResourceListener listener : listeners)
        {
            listener.managedConnectionPreClose();
        }

        try
        {
            Map<String, Table> oldtables = tables;
            tables = new HashMap<String, Table>();

            try
            {
                for (Table table : oldtables.values())
                {
                    table.close();
                }

                // Close the HConnection
                dispose();
            }
            catch (IOException e)
            {
                throw new NucleusDataStoreException("Exception thrown while closing HTable(s) for transaction and associated HConnection", e);
            }
        }
        finally
        {
            for (ManagedConnectionResourceListener listener : listeners)
            {
                listener.managedConnectionPostClose();
            }
        }

        super.close();
    }

    public void incrementUseCount()
    {
        super.incrementUseCount();
        disableExpirationTime();
    }

    public void release()
    {
        super.release();
        if (useCount == 0)
        {
            enableExpirationTime();
        }
    }

    private void enableExpirationTime()
    {
        this.expirationTime = System.currentTimeMillis() + idleTimeoutMills;
    }

    private void disableExpirationTime()
    {
        this.expirationTime = -1;
    }

    public void setIdleTimeoutMills(int mills)
    {
        this.idleTimeoutMills = mills;
    }

    public boolean isExpired()
    {
        return expirationTime > 0 && expirationTime > System.currentTimeMillis();
    }

    public void dispose()
    {
        isDisposed = true;
        if (!conn.isClosed())
        {
            try
            {
                conn.close();
            }
            catch (IOException e)
            {
                throw new NucleusDataStoreException("Exception thrown closing HConnection", e);
            }
        }
    }

    public boolean isDisposed()
    {
        return isDisposed;
    }
}