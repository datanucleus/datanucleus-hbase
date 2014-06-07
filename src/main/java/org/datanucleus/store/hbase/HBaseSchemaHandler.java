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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.hbase.metadata.MetaDataExtensionParser;
import org.datanucleus.store.schema.AbstractStoreSchemaHandler;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.CompleteClassTable;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Handler for schema management with HBase.
 */
public class HBaseSchemaHandler extends AbstractStoreSchemaHandler
{
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
                    createSchemaForClass((HBaseStoreManager) storeMgr, cmd, false);
                }
            }
        }
    }

    /**
     * Create a schema in HBase. Do not make this method public, since it uses privileged actions.
     * @param storeMgr HBase StoreManager
     * @param cmd Metadata for the class
     * @param validateOnly Whether to only validate for existence and flag missing schema in the log
     */
    public void createSchemaForClass(final HBaseStoreManager storeMgr, final AbstractClassMetaData cmd, final boolean validateOnly)
    {
        if (cmd.isEmbeddedOnly())
        {
            // No schema required since only ever embedded
            return;
        }

        StoreData storeData = storeMgr.getStoreDataForClass(cmd.getFullClassName());
        Table table = null;
        if (storeData != null)
        {
            table = storeData.getTable();
        }
        else
        {
            table = new CompleteClassTable(storeMgr, cmd, null);
        }
        final String tableName = table.getName();
        final Configuration config = storeMgr.getHbaseConfig();
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
                    HTableDescriptor hTable = null;
                    try
                    {
                        hTable = hBaseAdmin.getTableDescriptor(tableName.getBytes());
                    }
                    catch (TableNotFoundException ex)
                    {
                        if (validateOnly)
                        {
                            NucleusLogger.DATASTORE_SCHEMA.info(Localiser.msg("HBase.SchemaValidate.Class", cmd.getFullClassName(), tableName));
                        }
                        else if (storeMgr.getSchemaHandler().isAutoCreateTables())
                        {
                            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("HBase.SchemaCreate.Class", cmd.getFullClassName(), tableName));
                            hTable = new HTableDescriptor(tableName);
                            hBaseAdmin.createTable(hTable);
                        }
                    }
                    return hTable;
                }
            });

            // No such table & no auto-create -> exit
            if (hTable == null)
            {
                return;
            }
            MetaDataExtensionParser ep = new MetaDataExtensionParser(cmd);

            boolean modified = false;
            if (!hTable.hasFamily(tableName.getBytes()))
            {
                if (validateOnly)
                {
                    NucleusLogger.DATASTORE_SCHEMA.info(Localiser.msg("HBase.SchemaValidate.Class.Family", tableName, tableName));
                }
                else if (storeMgr.getSchemaHandler().isAutoCreateColumns())
                {
                    NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("HBase.SchemaCreate.Class.Family", tableName, tableName));

                    // Not sure this is good. This creates a default family even if the family is actually defined in the @Column(name="family:fieldname") annotation. 
                    // i.e it is possible to get a family with no fields.
                    HColumnDescriptor hColumn = new HColumnDescriptor(tableName);
                    hTable.addFamily(hColumn);
                    modified = true;
                }
            }

            List<Column> cols = table.getColumns();
            Set<String> familyNames = new HashSet<String>();
            for (Column col : cols)
            {
                String familyName = HBaseUtils.getFamilyNameForColumn(col);
                if (!familyNames.contains(familyName))
                {
                    familyNames.add(familyName);
                    if (!hTable.hasFamily(familyName.getBytes()))
                    {
                        if (validateOnly)
                        {
                            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("HBase.SchemaValidate.Class.Family", tableName, familyName));
                        }
                        else if (storeMgr.getSchemaHandler().isAutoCreateColumns())
                        {
                            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("HBase.SchemaCreate.Class.Family", tableName, familyName));
                            HColumnDescriptor hColumn = new HColumnDescriptor(familyName);
                            hTable.addFamily(hColumn);
                            modified = true;
                        }
                    }
                }
            }

            if (!validateOnly && ep.hasExtensions())
            {
                for (String familyName : familyNames)
                {
                    modified |= ep.applyExtensions(hTable, familyName);
                }
            }

            if (modified)
            {
                AccessController.doPrivileged(new PrivilegedExceptionAction()
                {
                    public Object run() throws Exception
                    {
                        hBaseAdmin.disableTable(hTable.getName());
                        hBaseAdmin.modifyTable(hTable.getName(), hTable);
                        hBaseAdmin.enableTable(hTable.getName());
                        return null;
                    }
                });
            }

        }
        catch (PrivilegedActionException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e.getCause());
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
     * @param storeMgr HBase StoreManager
     * @param cmd Metadata for the class
     */
    void deleteSchemaForClass(final AbstractClassMetaData cmd)
    {
        if (cmd.isEmbeddedOnly())
        {
            // No schema present since only ever embedded
            return;
        }

        StoreData storeData = storeMgr.getStoreDataForClass(cmd.getFullClassName());
        Table table = null;
        if (storeData != null)
        {
            table = storeData.getTable();
        }
        else
        {
            table = new CompleteClassTable(storeMgr, cmd, null);
        }
        final String tableName = table.getName();
        final Configuration config = ((HBaseStoreManager)storeMgr).getHbaseConfig();
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
                        // TODO Why create it if we are trying to delete it????
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
                    NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("HBase.SchemaDelete.Class", cmd.getFullClassName(), hTable.getNameAsString()));
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
                    createSchemaForClass((HBaseStoreManager) storeMgr, cmd, true);
                }
            }
        }
    }
}