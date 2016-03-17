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

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
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
     * Create a schema in HBase.
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
        Table t = null;
        if (storeData != null)
        {
            t = storeData.getTable();
        }
        else
        {
            t = new CompleteClassTable(storeMgr, cmd, null);
        }
        final Table table = t;
        final String tableNameString = table.getName();
        final TableName tableName = TableName.valueOf(tableNameString);

        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(-1);
        try
        {
            Connection conn = (Connection) mconn.getConnection();
            try
            {
                Admin hBaseAdmin = conn.getAdmin();

                // Find table descriptor, if not existing, create it
                HTableDescriptor hTable = null;
                try
                {
                    hTable = hBaseAdmin.getTableDescriptor(tableName);
                }
                catch (TableNotFoundException tnfe)
                {
                    if (validateOnly)
                    {
                        NucleusLogger.DATASTORE_SCHEMA.info(Localiser.msg("HBase.SchemaValidate.Class", cmd.getFullClassName(), tableNameString));
                    }
                    else if (storeMgr.getSchemaHandler().isAutoCreateTables())
                    {
                        NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("HBase.SchemaCreate.Class", cmd.getFullClassName(), tableNameString));
                        hTable = new HTableDescriptor(tableName);
                        populateHTableColumnFamilyNames(hTable, table);
                        hBaseAdmin.createTable(hTable);
                    }
                }

                // No such table & no auto-create -> exit
                if (hTable == null)
                {
                    return;
                }

                boolean modified = false;

                List<Column> cols = table.getColumns();
                Set<String> familyNames = new HashSet<String>();
                for (Column col : cols)
                {
                    boolean changed = addColumnFamilyForColumn(col, hTable, tableNameString, familyNames, validateOnly);
                    if (changed)
                    {
                        modified = true;
                    }
                }
                if (table.getDatastoreIdColumn() != null)
                {
                    boolean changed = addColumnFamilyForColumn(table.getDatastoreIdColumn(), hTable, tableNameString, familyNames, validateOnly);
                    if (changed)
                    {
                        modified = true;
                    }
                }
                if (table.getVersionColumn() != null)
                {
                    boolean changed = addColumnFamilyForColumn(table.getVersionColumn(), hTable, tableNameString, familyNames, validateOnly);
                    if (changed)
                    {
                        modified = true;
                    }
                }
                if (table.getDiscriminatorColumn() != null)
                {
                    boolean changed = addColumnFamilyForColumn(table.getDiscriminatorColumn(), hTable, tableNameString, familyNames, validateOnly);
                    if (changed)
                    {
                        modified = true;
                    }
                }
                if (table.getMultitenancyColumn() != null)
                {
                    boolean changed = addColumnFamilyForColumn(table.getMultitenancyColumn(), hTable, tableNameString, familyNames, validateOnly);
                    if (changed)
                    {
                        modified = true;
                    }
                }

                // Process any extensions
                MetaDataExtensionParser ep = new MetaDataExtensionParser(cmd);
                if (!validateOnly && ep.hasExtensions())
                {
                    for (String familyName : familyNames)
                    {
                        modified |= ep.applyExtensions(hTable, familyName);
                    }
                }

                if (modified)
                {
                    hBaseAdmin.disableTable(tableName);
                    hBaseAdmin.modifyTable(tableName, hTable);
                    hBaseAdmin.enableTable(tableName);
                }
            }
            catch (IOException e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e.getCause());
            }
        }
        finally
        {
            mconn.release();
        }
    }

    protected void populateHTableColumnFamilyNames(HTableDescriptor hTable, final Table table)
    {
        Set<String> familyNames = new HashSet<String>();
        for (Column col : table.getColumns())
        {
            String familyName = HBaseUtils.getFamilyNameForColumn(col);
            if (familyNames.add(familyName)) {
                HColumnDescriptor familyNameCd = new HColumnDescriptor(familyName);
                hTable.addFamily(familyNameCd);
            }
        }
    }

    protected boolean addColumnFamilyForColumn(Column col, HTableDescriptor htable, String tableName, Set<String> familyNames, boolean validateOnly)
    {
        boolean modified = false;

        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        if (!familyNames.contains(familyName))
        {
            familyNames.add(familyName);
            if (!htable.hasFamily(familyName.getBytes()))
            {
                if (validateOnly)
                {
                    NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("HBase.SchemaValidate.Class.Family", tableName, familyName));
                }
                else if (storeMgr.getSchemaHandler().isAutoCreateColumns())
                {
                    NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("HBase.SchemaCreate.Class.Family", tableName, familyName));
                    HColumnDescriptor hColumn = new HColumnDescriptor(familyName);
                    htable.addFamily(hColumn);
                    modified = true;
                }
            }
        }
        return modified;
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
        final String tableNameString = table.getName();
        final TableName tableName = TableName.valueOf(tableNameString);

        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(-1);
        try
        {
            Connection conn = (Connection) mconn.getConnection();
            try
            {
                Admin hBaseAdmin = conn.getAdmin();

                // Find table descriptor, if not existing, create it
                HTableDescriptor hTable = null;
                try
                {
                    hTable = hBaseAdmin.getTableDescriptor(tableName);
                }
                catch (TableNotFoundException tnfe)
                {
                    // TODO Why create it if we are trying to delete it????
                    hTable = new HTableDescriptor(tableName);
                    hBaseAdmin.createTable(hTable);
                }

                NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("HBase.SchemaDelete.Class", cmd.getFullClassName(), hTable.getNameAsString()));
                hBaseAdmin.disableTable(tableName);
                hBaseAdmin.deleteTable(tableName);
            }
            catch (IOException e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e.getCause());
            }
        }
        finally
        {
            mconn.release();
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