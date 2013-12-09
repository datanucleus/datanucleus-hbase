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
    ...
***********************************************************************/
package org.datanucleus.store.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.xa.XAResource;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;

/**
 * Implementation of a ManagedConnection.
 */
public class HBaseManagedConnection extends AbstractManagedConnection
{
    private HTablePool tablePool;

    /** Cache of HTables used by this connection. */
    private Map<String, HTableInterface> tables;

    private int idleTimeoutMills = 30 * 1000; // 30 secs

    private long expirationTime;

    private boolean isDisposed = false;

    public HBaseManagedConnection(HTablePool pool)
    {
    	this.tablePool = pool;
    	this.tables = new HashMap<String, HTableInterface>();
    	disableExpirationTime();
    }

    public Object getConnection()
    {
    	throw new NucleusDataStoreException("Unsupported Exception #getConnection() for " + this.getClass().getName());
    }

    public HTableInterface getHTable(String tableName)
    {
        HTableInterface table = tables.get(tableName);
        if (table == null)
        {
            try
            {
                table = tablePool.getTable(tableName);
                tables.put(tableName, table);
            }
        	catch (RuntimeException e)
        	{
                throw new NucleusDataStoreException(e.getMessage(),e);
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
        if (tables.size() == 0)
        {
            return;
        }

        for (ManagedConnectionResourceListener listener : listeners) {
            listener.managedConnectionPreClose();
        }
        try
        {
            Map<String, HTableInterface> oldtables = tables;
            tables = new HashMap<String, HTableInterface>();

            for (String tableName : oldtables.keySet())
            {
                try
                {
                    HTableInterface table = oldtables.get(tableName);
                    table.close();
                }
                catch (IOException e)
                {
                    // TODO: There can be multiple exceptions, so wrap them in together
                    throw new NucleusDataStoreException(e.getMessage(),e);
                }
            }
        }
        finally
        {
            for (ManagedConnectionResourceListener listener : listeners) {
                listener.managedConnectionPostClose();
            }
        }
    }

    protected void incrementUseCount()
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
    	return expirationTime > 0  &&  expirationTime > System.currentTimeMillis();
    }

    public void dispose()
    {
    	isDisposed = true;
    }

    public boolean isDisposed()
    {
    	return isDisposed;
    }
}