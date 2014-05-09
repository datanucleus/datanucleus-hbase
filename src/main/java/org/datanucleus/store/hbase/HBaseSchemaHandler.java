/**********************************************************************
Copyright (c) 2014 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
**********************************************************************/
package org.datanucleus.store.hbase;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.schema.AbstractStoreSchemaHandler;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Handler for schema management with HBase.
 */
public class HBaseSchemaHandler extends AbstractStoreSchemaHandler
{
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.store.hbase.Localisation", HBaseStoreManager.class.getClassLoader());

    public HBaseSchemaHandler(StoreManager storeMgr)
    {
        super(storeMgr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.AbstractStoreSchemaHandler#createSchemaForClasses(java.util.Set, java.util.Properties, java.lang.Object)
     */
    @Override
    public void createSchemaForClasses(Set<String> classNames, Properties props, Object connection)
    {
        if (isAutoCreateTables() || isAutoCreateColumns())
        {
            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    HBaseUtils.createSchemaForClass((HBaseStoreManager) storeMgr, cmd, false);
                }
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.AbstractStoreSchemaHandler#deleteSchemaForClasses(java.util.Set, java.util.Properties, java.lang.Object)
     */
    @Override
    public void deleteSchemaForClasses(Set<String> classNames, Properties props, Object connection)
    {
        Iterator<String> classIter = classNames.iterator();
        ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
        while (classIter.hasNext())
        {
            String className = classIter.next();
            AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
            if (cmd != null)
            {
                deleteSchemaForClass(cmd);
            }
        }
    }

    /**
     * Delete the schema for the specified class from HBase.
     * Do not make this method public, since it uses privileged actions
     * TODO Move to HBaseSchemaHandler
     * @param storeMgr HBase StoreManager
     * @param acmd Metadata for the class
     */
    void deleteSchemaForClass(final AbstractClassMetaData acmd)
    {
        if (acmd.isEmbeddedOnly())
        {
            // No schema present since only ever embedded
            return;
        }

        HBaseStoreManager hbaseStoreMgr = (HBaseStoreManager)storeMgr;
        final String tableName = storeMgr.getNamingFactory().getTableName(acmd);
        final Configuration config = hbaseStoreMgr.getHbaseConfig();
        try
        {
            final HBaseAdmin hBaseAdmin = (HBaseAdmin) AccessController.doPrivileged(new PrivilegedExceptionAction()
            {
                public Object run() throws Exception
                {
                    return new HBaseAdmin(config);
                }
            });

            // Find table descriptor, if not existing, create it
            final HTableDescriptor hTable = (HTableDescriptor) AccessController.doPrivileged(new PrivilegedExceptionAction()
            {
                public Object run() throws Exception
                {
                    HTableDescriptor hTable;
                    try
                    {
                        hTable = hBaseAdmin.getTableDescriptor(tableName.getBytes());
                    }
                    catch (TableNotFoundException ex)
                    {
                        hTable = new HTableDescriptor(tableName);
                        hBaseAdmin.createTable(hTable);
                    }
                    return hTable;
                }
            });

            AccessController.doPrivileged(new PrivilegedExceptionAction()
            {
                public Object run() throws Exception
                {
                    NucleusLogger.DATASTORE_SCHEMA.debug(LOCALISER.msg("HBase.SchemaDelete.Class",
                        acmd.getFullClassName(), hTable.getNameAsString()));
                    hBaseAdmin.disableTable(hTable.getName());
                    hBaseAdmin.deleteTable(hTable.getName());
                    return null;
                }
            });
        }
        catch (PrivilegedActionException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e.getCause());
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.AbstractStoreSchemaHandler#validateSchema(java.util.Set, java.util.Properties, java.lang.Object)
     */
    @Override
    public void validateSchema(Set<String> classNames, Properties props, Object connection)
    {
        if (isValidateTables() || isValidateColumns())
        {
            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    HBaseUtils.createSchemaForClass((HBaseStoreManager) storeMgr, cmd, true);
                }
            }
        }
    }
}