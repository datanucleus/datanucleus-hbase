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
    ...
 ***********************************************************************/
package org.datanucleus.store.hbase;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Pool of HBaseManagedConnection, so that we only have one per thread of any application per PMF/EMF.
 */
public class HBaseConnectionPool
{
    private final List<HBaseManagedConnection> connections;

    private final ThreadLocal<WeakReference<HBaseManagedConnection>> connectionForCurrentThread;

    private final Timer evictorThread;

    private int timeBetweenEvictionRunsMillis = 15 * 1000; // default, 15 secs

    public HBaseConnectionPool()
    {
        connectionForCurrentThread = new ThreadLocal<WeakReference<HBaseManagedConnection>>();
        connections = new CopyOnWriteArrayList<HBaseManagedConnection>();

        // Start evictor thread
        evictorThread = new Timer("HBase Connection Evictor", true);
        TimerTask timeoutTask = new TimerTask()
        {
            public void run()
            {
                disposeTimedOutConnections();
            }
        };
        evictorThread.schedule(timeoutTask, timeBetweenEvictionRunsMillis, timeBetweenEvictionRunsMillis);
    }

    public void registerConnection(HBaseManagedConnection managedConnection)
    {
        connections.add(managedConnection);
        connectionForCurrentThread.set(new WeakReference<HBaseManagedConnection>(managedConnection));
    }

    public HBaseManagedConnection getPooledConnection()
    {
        WeakReference<HBaseManagedConnection> ref = connectionForCurrentThread.get();
        if (ref == null)
        {
            return null;
        }

        HBaseManagedConnection managedConnection = ref.get();
        if (managedConnection != null && !managedConnection.isDisposed())
        {
            return managedConnection;
        }

        return null;
    }

    public void setTimeBetweenEvictionRunsMillis(int timeBetweenEvictionRunsMillis)
    {
        this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
    }

    private void disposeTimedOutConnections()
    {
        List<HBaseManagedConnection> timedOutConnections = new ArrayList<HBaseManagedConnection>();

        for (HBaseManagedConnection managedConnection : connections)
        {
            if (managedConnection.isExpired())
            {
                timedOutConnections.add(managedConnection);
            }
        }

        for (HBaseManagedConnection managedConnection : timedOutConnections)
        {
            managedConnection.dispose();
            connections.remove(managedConnection);
        }
    }
}