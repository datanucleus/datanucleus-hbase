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
2011 Andy Jefferson - support for datastore identity, versions, discriminators,
                      localisation
    ...
***********************************************************************/
package org.datanucleus.store.hbase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.OID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.VersionHelper;
import org.datanucleus.store.fieldmanager.DeleteFieldManager;
import org.datanucleus.store.hbase.fieldmanager.FetchFieldManager;
import org.datanucleus.store.hbase.fieldmanager.StoreFieldManager;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Persistence handler for HBase, providing insert, update, delete, and find handling.
 */
public class HBasePersistenceHandler extends AbstractPersistenceHandler
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER_HBASE = Localiser.getInstance(
        "org.datanucleus.store.hbase.Localisation", HBaseStoreManager.class.getClassLoader());

    public HBasePersistenceHandler(StoreManager storeMgr)
    {
        super(storeMgr);
    }

    public void close()
    {
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractPersistenceHandler#insertObjects(org.datanucleus.store.ObjectProvider[])
     */
    @Override
    public void insertObjects(ObjectProvider... ops)
    {
        if (ops.length == 1)
        {
            insertObject(ops[0]);
            return;
        }

        // TODO Implement any bulk insert
        super.insertObjects(ops);
    }

    public void insertObject(ObjectProvider op)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        if (!storeMgr.managesClass(op.getClassMetaData().getFullClassName()))
        {
            storeMgr.manageClasses(op.getExecutionContext().getClassLoaderResolver(), op.getClassMetaData().getFullClassName());
        }

        ExecutionContext ec = op.getExecutionContext();
        AbstractClassMetaData cmd = op.getClassMetaData();
        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(ec);
        try
        {
            String tableName = storeMgr.getNamingFactory().getTableName(cmd);
            HTableInterface table = mconn.getHTable(tableName);
            boolean enforceUniquenessInApp = storeMgr.getBooleanProperty("datanucleus.hbase.enforceUniquenessInApplication", false);
            if (enforceUniquenessInApp)
            {
                NucleusLogger.DATASTORE_PERSIST.info("User requesting to enforce uniqueness of object identity in their application, so not checking for existence");
            }
            else
            {
                // Check for existence of this object identity in the datastore
                if (cmd.getIdentityType() == IdentityType.APPLICATION || cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    try
                    {
                        if (HBaseUtils.objectExistsInTable(op, table))
                        {
                            throw new NucleusUserException(LOCALISER_HBASE.msg("HBase.Insert.ObjectWithIdAlreadyExists", 
                                op.getObjectAsPrintable(), op.getInternalObjectId()));
                        }
                    }
                    catch (IOException e)
                    {
                        throw new NucleusDataStoreException(e.getMessage(), e);
                    }
                }
            }

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_HBASE.msg("HBase.Insert.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            Put put = HBaseUtils.getPutForObject(op);
            Delete delete = HBaseUtils.getDeleteForObject(op);

            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                String colName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.DATASTOREID_COLUMN);
                String familyName = HBaseUtils.getFamilyNameForColumnName(colName, tableName);
                String qualifName = HBaseUtils.getQualifierNameForColumnName(colName);
                Object key = ((OID)op.getInternalObjectId()).getKeyValue();
                try
                {
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(bos);
                    oos.writeObject(key);
                    oos.flush();
                    put.add(familyName.getBytes(), qualifName.getBytes(), bos.toByteArray());
                    oos.close();
                    bos.close();
                }
                catch (IOException e)
                {
                    throw new NucleusException(e.getMessage(), e);
                }
            }

            if (cmd.hasDiscriminatorStrategy())
            {
                // Add discriminator field
                DiscriminatorMetaData discmd = cmd.getDiscriminatorMetaData();
                String discVal = null;
                if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.CLASS_NAME)
                {
                    discVal = cmd.getFullClassName();
                }
                else
                {
                    discVal = discmd.getValue();
                }
                String colName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.DISCRIMINATOR_COLUMN);
                String familyName = HBaseUtils.getFamilyNameForColumnName(colName, tableName);
                String qualifName = HBaseUtils.getQualifierNameForColumnName(colName);

                put.add(familyName.getBytes(), qualifName.getBytes(), discVal.getBytes());
            }

            if (storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID) != null)
            {
                if ("true".equalsIgnoreCase(cmd.getValueForExtension("multitenancy-disable")))
                {
                    // Don't bother with multitenancy for this class
                }
                else
                {
                    String name = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.MULTITENANCY_COLUMN);
                    String familyName = HBaseUtils.getFamilyNameForColumnName(name, tableName);
                    String qualifName = HBaseUtils.getQualifierNameForColumnName(name);
                    put.add(familyName.getBytes(), qualifName.getBytes(),
                        storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID).getBytes());
                }
            }

            if (cmd.isVersioned())
            {
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                String colName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.VERSION_COLUMN);
                String familyName = HBaseUtils.getFamilyNameForColumnName(colName, tableName);
                String qualifName = HBaseUtils.getQualifierNameForColumnName(colName);
                if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                {
                    long versionNumber = 1;
                    op.setTransactionalVersion(Long.valueOf(versionNumber));
                    if (NucleusLogger.DATASTORE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE.debug(LOCALISER_HBASE.msg("HBase.Insert.ObjectPersistedWithVersion",
                            op.getObjectAsPrintable(), op.getInternalObjectId(), "" + versionNumber));
                    }
                    if (vermd.getFieldName() != null)
                    {
                        AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                        Object verFieldValue = Long.valueOf(versionNumber);
                        if (verMmd.getType() == int.class || verMmd.getType() == Integer.class)
                        {
                            verFieldValue = Integer.valueOf((int)versionNumber);
                        }
                        op.replaceField(verMmd.getAbsoluteFieldNumber(), verFieldValue);
                    }
                    else
                    {
                        put.add(familyName.getBytes(), qualifName.getBytes(), Bytes.toBytes(versionNumber));
                    }
                }
                else if (vermd.getVersionStrategy() == VersionStrategy.DATE_TIME)
                {
                    Date date = new Date();
                    Timestamp ts = new Timestamp(date.getTime());
                    op.setTransactionalVersion(ts);
                    if (NucleusLogger.DATASTORE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE.debug(LOCALISER_HBASE.msg("HBase.Insert.ObjectPersistedWithVersion",
                            op.getObjectAsPrintable(), op.getInternalObjectId(), "" + ts));
                    }
                    if (vermd.getFieldName() != null)
                    {
                        AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                        op.replaceField(verMmd.getAbsoluteFieldNumber(), ts);
                    }
                    else
                    {
                        try
                        {
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(ts);
                            put.add(familyName.getBytes(), qualifName.getBytes(), bos.toByteArray());
                            oos.close();
                            bos.close();
                        }
                        catch (IOException e)
                        {
                            throw new NucleusException(e.getMessage(), e);
                        }
                    }
                }
            }

            StoreFieldManager fm = new StoreFieldManager(op, put, delete, true, tableName);
            op.provideFields(cmd.getAllMemberPositions(), fm);

            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug(LOCALISER_HBASE.msg("HBase.Put",
                    StringUtils.toJVMIDString(op.getObject()), tableName, put));
            }
            table.put(put);
            if (ec.getStatistics() != null)
            {
                // Add to statistics
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementInsertCount();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_HBASE.msg("HBase.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
        }
        catch (NucleusException ne)
        {
            throw ne;
        }
        catch (Exception e)
        {
            NucleusLogger.DATASTORE_PERSIST.warn("Exception thrown on insert", e);
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    public void updateObject(ObjectProvider op, int[] fieldNumbers)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        ExecutionContext ec = op.getExecutionContext();
        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(ec);
        try
        {
            long startTime = System.currentTimeMillis();
            AbstractClassMetaData cmd = op.getClassMetaData();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                StringBuffer fieldStr = new StringBuffer();
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        fieldStr.append(",");
                    }
                    fieldStr.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_HBASE.msg("HBase.Update.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId(), fieldStr.toString()));
            }

            String tableName = storeMgr.getNamingFactory().getTableName(cmd);
            HTableInterface table = mconn.getHTable(tableName);
            if (cmd.isVersioned())
            {
                // Optimistic checking of version
                Result result = HBaseUtils.getResultForObject(op, table);
                Object datastoreVersion = HBaseUtils.getVersionForObject(cmd, result, ec, tableName,
                    storeMgr);
                VersionHelper.performVersionCheck(op, datastoreVersion, cmd.getVersionMetaDataForClass());
            }

            int[] updatedFieldNums = fieldNumbers;
            Put put = HBaseUtils.getPutForObject(op);
            Delete delete = HBaseUtils.getDeleteForObject(op);
            if (cmd.isVersioned())
            {
                // Version object so calculate version to store with
                Object currentVersion = op.getTransactionalVersion();
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                Object nextVersion = VersionHelper.getNextVersion(vermd.getVersionStrategy(), currentVersion);
                op.setTransactionalVersion(nextVersion);

                if (vermd.getFieldName() != null)
                {
                    // Update the field version value
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    op.replaceField(verMmd.getAbsoluteFieldNumber(), nextVersion);
                    boolean updatingVerField = false;
                    for (int i=0;i<fieldNumbers.length;i++)
                    {
                        if (fieldNumbers[i] == verMmd.getAbsoluteFieldNumber())
                        {
                            updatingVerField = true;
                        }
                    }
                    if (!updatingVerField)
                    {
                        // Add the version field to the fields to be updated
                        updatedFieldNums = new int[fieldNumbers.length+1];
                        System.arraycopy(fieldNumbers, 0, updatedFieldNums, 0, fieldNumbers.length);
                        updatedFieldNums[fieldNumbers.length] = verMmd.getAbsoluteFieldNumber();
                    }
                }
                else
                {
                    // Update the stored surrogate value
                    String colName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.VERSION_COLUMN);
                    String familyName = HBaseUtils.getFamilyNameForColumnName(colName, tableName);
                    String qualifName = HBaseUtils.getQualifierNameForColumnName(colName);
                    if (nextVersion instanceof Long)
                    {
                        put.add(familyName.getBytes(), qualifName.getBytes(), Bytes.toBytes((Long)nextVersion));
                    }
                    else if (nextVersion instanceof Integer)
                    {
                        put.add(familyName.getBytes(), qualifName.getBytes(), Bytes.toBytes((Integer)nextVersion));
                    }
                    else
                    {
                        put.add(familyName.getBytes(), qualifName.getBytes(), ((String)nextVersion).getBytes());
                    }
                }
            }

            StoreFieldManager fm = new StoreFieldManager(op, put, delete, false, tableName);
            op.provideFields(updatedFieldNums, fm);
            if (!put.isEmpty())
            {
                if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_NATIVE.debug(LOCALISER_HBASE.msg("HBase.Put",
                        StringUtils.toJVMIDString(op.getObject()), tableName, put));
                }
                table.put(put);
                if (ec.getStatistics() != null)
                {
                    // Add to statistics
                    ec.getStatistics().incrementNumWrites();
                }
            }
            if (!delete.isEmpty())
            {
                // only delete if there are columns to delete. Otherwise an empty delete would cause the
                // entire row to be deleted
                if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_NATIVE.debug(LOCALISER_HBASE.msg("HBase.Delete",
                        StringUtils.toJVMIDString(op.getObject()), tableName, delete));
                }
                table.delete(delete);
                if (ec.getStatistics() != null)
                {
                    // Add to statistics
                    ec.getStatistics().incrementNumWrites();
                }
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementUpdateCount();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_HBASE.msg("HBase.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
        }
        catch (NucleusException ne)
        {
            throw ne;
        }
        catch (Exception e)
        {
            NucleusLogger.DATASTORE_PERSIST.warn("Exception thrown on update", e);
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractPersistenceHandler#deleteObjects(org.datanucleus.store.ObjectProvider[])
     */
    @Override
    public void deleteObjects(ObjectProvider... ops)
    {
        if (ops.length == 1)
        {
            deleteObject(ops[0]);
            return;
        }

        // Separate the objects to be deleted into groups, for the "table" in question
        Map<String, Set<ObjectProvider>> opsByTable = new HashMap<String, Set<ObjectProvider>>();
        for (int i=0;i<ops.length;i++)
        {
            AbstractClassMetaData cmd = ops[i].getClassMetaData();
            String tableName = storeMgr.getNamingFactory().getTableName(cmd);
            Set<ObjectProvider> opsForTable = opsByTable.get(tableName);
            if (opsForTable == null)
            {
                opsForTable = new HashSet<ObjectProvider>();
                opsByTable.put(tableName, opsForTable);
            }
            opsForTable.add(ops[i]);
        }

        Set<NucleusOptimisticException> optimisticExcps = null;
        for (Map.Entry<String, Set<ObjectProvider>> entry : opsByTable.entrySet())
        {
            String tableName = entry.getKey();
            Set<ObjectProvider> opsForTable = entry.getValue();
            ExecutionContext ec = ops[0].getExecutionContext();
            HBaseManagedConnection mconn = (HBaseManagedConnection)storeMgr.getConnection(ec);
            try
            {
                long startTime = System.currentTimeMillis();
                HTableInterface table = mconn.getHTable(tableName);

                List<Delete> deletes = new ArrayList(opsForTable.size());
                for (ObjectProvider op : opsForTable)
                {
                    // Check if read-only so update not permitted
                    assertReadOnlyForUpdateOfObject(op);

                    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_HBASE.msg("HBase.Delete.Start", 
                            op.getObjectAsPrintable(), op.getInternalObjectId()));
                    }
                    boolean deletable = true;
                    AbstractClassMetaData cmd = op.getClassMetaData();
                    if (cmd.isVersioned())
                    {
                        // Optimistic checking of version
                        Object currentVersion = op.getTransactionalVersion();
                        Result result = HBaseUtils.getResultForObject(op, table);
                        Object datastoreVersion = HBaseUtils.getVersionForObject(cmd, result, ec, tableName, storeMgr);
                        if (!datastoreVersion.equals(currentVersion)) // TODO Use VersionHelper.performVersionCheck
                        {
                            if (optimisticExcps == null)
                            {
                                optimisticExcps = new HashSet<NucleusOptimisticException>();
                            }
                            optimisticExcps.add(new NucleusOptimisticException("Cannot delete object with id " + 
                                op.getInternalObjectId() + " since has version=" + currentVersion + 
                                " while datastore has version=" + datastoreVersion));
                            deletable = false;
                        }
                    }

                    if (deletable)
                    {
                        // Invoke any cascade deletion
                        op.loadUnloadedFields();
                        op.provideFields(cmd.getAllMemberPositions(), new DeleteFieldManager(op));

                        // Delete the object
                        deletes.add(HBaseUtils.getDeleteForObject(op));
                    }
                }

                // Delete all rows from this table
                table.delete(deletes);
                if (ec.getStatistics() != null)
                {
                    // Add to statistics
                    ec.getStatistics().incrementNumWrites();
                    for (int i=0;i<opsForTable.size();i++)
                    {
                        ec.getStatistics().incrementDeleteCount();
                    }
                }

                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_HBASE.msg("HBase.ExecutionTime", 
                        (System.currentTimeMillis() - startTime)));
                }
            }
            catch (NucleusException ne)
            {
                throw ne;
            }
            catch (Exception e)
            {
                NucleusLogger.DATASTORE_PERSIST.warn("Exception thrown on delete", e);
                throw new NucleusDataStoreException(e.getMessage(), e);
            }
            finally
            {
                mconn.release();
            }
        }

        if (optimisticExcps != null)
        {
            if (optimisticExcps.size() == 1)
            {
                throw optimisticExcps.iterator().next();
            }
            throw new NucleusOptimisticException("Optimistic exceptions thrown during delete of objects",
                optimisticExcps.toArray(new NucleusOptimisticException[optimisticExcps.size()]));
        }
    }

    public void deleteObject(ObjectProvider op)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        ExecutionContext ec = op.getExecutionContext();
        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(ec);
        try
        {
            AbstractClassMetaData cmd = op.getClassMetaData();
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_HBASE.msg("HBase.Delete.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            String tableName = storeMgr.getNamingFactory().getTableName(cmd);
            HTableInterface table = mconn.getHTable(tableName);
            if (cmd.isVersioned())
            {
                // Optimistic checking of version
                Result result = HBaseUtils.getResultForObject(op, table);
                Object datastoreVersion = HBaseUtils.getVersionForObject(cmd, result, ec, tableName, storeMgr);
                VersionHelper.performVersionCheck(op, datastoreVersion, cmd.getVersionMetaDataForClass());
            }

            // Invoke any cascade deletion
            op.loadUnloadedFields();
            op.provideFields(cmd.getAllMemberPositions(), new DeleteFieldManager(op));

            // Delete the object
            Delete delete = HBaseUtils.getDeleteForObject(op);
            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug(LOCALISER_HBASE.msg("HBase.Delete",
                    StringUtils.toJVMIDString(op.getObject()), tableName, delete));
            }
            table.delete(delete);
            if (ec.getStatistics() != null)
            {
                // Add to statistics
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementDeleteCount();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_HBASE.msg("HBase.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
        }
        catch (NucleusException ne)
        {
            throw ne;
        }
        catch (Exception e)
        {
            NucleusLogger.DATASTORE_PERSIST.warn("Exception thrown on delete", e);
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    public void fetchObject(ObjectProvider op, int[] fieldNumbers)
    {
        ExecutionContext ec = op.getExecutionContext();
        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(ec);
        try
        {
            AbstractClassMetaData cmd = op.getClassMetaData();
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                // Debug information about what we are retrieving
                StringBuffer str = new StringBuffer("Fetching object \"");
                str.append(op.getObjectAsPrintable()).append("\" (id=");
                str.append(op.getInternalObjectId()).append(")").append(" fields [");
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        str.append(",");
                    }
                    str.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                str.append("]");
                NucleusLogger.PERSISTENCE.debug(str.toString());
            }

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER_HBASE.msg("HBase.Fetch.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            String tableName = storeMgr.getNamingFactory().getTableName(cmd);
            HTableInterface table = mconn.getHTable(tableName);
            Result result = HBaseUtils.getResultForObject(op, table);
            if (result.getRow() == null)
            {
                throw new NucleusObjectNotFoundException();
            }
            else if (cmd.hasDiscriminatorStrategy())
            {
                String colName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.DISCRIMINATOR_COLUMN);
                String familyName = HBaseUtils.getFamilyNameForColumnName(colName, tableName);
                String columnName = HBaseUtils.getQualifierNameForColumnName(colName);
                Object discValue = new String(result.getValue(familyName.getBytes(), columnName.getBytes()));
                if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.CLASS_NAME)
                {
                    if (!cmd.getFullClassName().equals(discValue))
                    {
                        throw new NucleusObjectNotFoundException();
                    }
                }
                else if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.VALUE_MAP)
                {
                    if (!cmd.getDiscriminatorValue().equals(discValue))
                    {
                        throw new NucleusObjectNotFoundException();
                    }
                }
            }

            FetchFieldManager fm = new FetchFieldManager(op, result, tableName);
            op.replaceFields(cmd.getAllMemberPositions(), fm);

            if (cmd.isVersioned() && op.getTransactionalVersion() == null)
            {
                // No version set, so retrieve it
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                if (vermd.getFieldName() != null)
                {
                    // Version stored in a field
                    Object datastoreVersion =
                        op.provideField(cmd.getAbsolutePositionOfMember(vermd.getFieldName()));
                    op.setVersion(datastoreVersion);
                }
                else
                {
                    // Surrogate version
                    op.setVersion(HBaseUtils.getSurrogateVersionForObject(cmd, result, tableName, storeMgr));
                }
            }

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER_HBASE.msg("HBase.ExecutionTime",
                    (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementFetchCount();
            }
        }
        catch (NucleusException ne)
        {
            throw ne;
        }
        catch (Exception e)
        {
            NucleusLogger.DATASTORE_PERSIST.warn("Exception thrown on fetch", e);
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    public Object findObject(ExecutionContext ectx, Object id)
    {
        return null;
    }

    public void locateObject(ObjectProvider op)
    {
        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(op.getExecutionContext());
        try
        {
            final AbstractClassMetaData cmd = op.getClassMetaData();
            String tableName = storeMgr.getNamingFactory().getTableName(cmd);
            HTableInterface table = mconn.getHTable(tableName);
            if (!HBaseUtils.objectExistsInTable(op, table))
            {
                throw new NucleusObjectNotFoundException();
            }
        }
        catch (NucleusException ne)
        {
            throw ne;
        }
        catch (Exception e)
        {
            NucleusLogger.DATASTORE_PERSIST.warn("Exception thrown on existence check", e);
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractPersistenceHandler#locateObjects(org.datanucleus.state.ObjectProvider[])
     */
    @Override
    public void locateObjects(ObjectProvider[] ops)
    {
        // TODO This requires HBase 0.95 which adds exists(List) method.
        /*// Separate the objects to be persisted into groups, for the "table" in question
        Map<String, Set<ObjectProvider>> opsByTable = new HashMap();
        for (int i=0;i<ops.length;i++)
        {
            AbstractClassMetaData cmd = ops[i].getClassMetaData();
            if (!cmd.pkIsDatastoreAttributed(storeMgr))
            {
                String tableName = storeMgr.getNamingFactory().getTableName(cmd);
                Set<ObjectProvider> opsForTable = opsByTable.get(tableName);
                if (opsForTable == null)
                {
                    opsForTable = new HashSet<ObjectProvider>();
                    opsByTable.put(tableName, opsForTable);
                }
                opsForTable.add(ops[i]);
            }
        }

        ExecutionContext ec = ops[0].getExecutionContext();
        for (String tableName : opsByTable.keySet())
        {
            Set<ObjectProvider> opsForTable = opsByTable.get(tableName);
            HBaseManagedConnection mconn = (HBaseManagedConnection)storeMgr.getConnection(ec);
            try
            {
                HTableInterface table = mconn.getHTable(tableName);
                List<Get> gets = new ArrayList<Get>(opsForTable.size());
                for (ObjectProvider op : opsForTable)
                {
                    Get get = HBaseUtils.getGetForObject(op);
                    gets.add(get);
                }
                table.exists(gets);
            }
            catch (Exception e)
            {
                NucleusLogger.PERSISTENCE.error("Exception locating objects", e);
                throw new NucleusDataStoreException("Exception locating objects", e);
            }
            finally
            {
                mconn.release();
            }
        }*/

        // TODO Auto-generated method stub
        super.locateObjects(ops);
    }
}