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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Delete;
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
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.VersionHelper;
import org.datanucleus.store.fieldmanager.DeleteFieldManager;
import org.datanucleus.store.hbase.fieldmanager.FetchFieldManager;
import org.datanucleus.store.hbase.fieldmanager.StoreFieldManager;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Persistence handler for HBase, providing insert, update, delete, and find handling.
 */
public class HBasePersistenceHandler extends AbstractPersistenceHandler
{
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

        ExecutionContext ec = op.getExecutionContext();
        AbstractClassMetaData cmd = op.getClassMetaData();
        if (!storeMgr.managesClass(op.getClassMetaData().getFullClassName()))
        {
            storeMgr.manageClasses(op.getExecutionContext().getClassLoaderResolver(), op.getClassMetaData().getFullClassName());
        }
        Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();

        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(ec);
        try
        {
            String tableName = table.getName();
            org.apache.hadoop.hbase.client.Table htable = mconn.getHTable(tableName);
            boolean enforceUniquenessInApp = storeMgr.getBooleanProperty(HBaseStoreManager.PROPERTY_HBASE_ENFORCE_UNIQUENESS_IN_APPLICATION, false);
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
                        if (HBaseUtils.objectExistsInTable(op, htable))
                        {
                            throw new NucleusUserException(Localiser.msg("HBase.Insert.ObjectWithIdAlreadyExists", op.getObjectAsPrintable(), op.getInternalObjectId()));
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
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("HBase.Insert.Start", op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            Put put = HBaseUtils.getPutForObject(op);
            Delete delete = HBaseUtils.getDeleteForObject(op);

            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                String familyName = HBaseUtils.getFamilyNameForColumn(table.getDatastoreIdColumn());
                String qualifName = HBaseUtils.getQualifierNameForColumn(table.getDatastoreIdColumn());
                Object key = IdentityUtils.getTargetKeyForDatastoreIdentity(op.getInternalObjectId());
                try
                {
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(bos);
                    oos.writeObject(key);
                    oos.flush();
                    put.addColumn(familyName.getBytes(), qualifName.getBytes(), bos.toByteArray());
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
                String familyName = HBaseUtils.getFamilyNameForColumn(table.getDiscriminatorColumn());
                String qualifName = HBaseUtils.getQualifierNameForColumn(table.getDiscriminatorColumn());

                put.addColumn(familyName.getBytes(), qualifName.getBytes(), discVal.getBytes());
            }

            if (storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID) != null)
            {
                if ("true".equalsIgnoreCase(cmd.getValueForExtension("multitenancy-disable")))
                {
                    // Don't bother with multitenancy for this class
                }
                else
                {
                    String familyName = HBaseUtils.getFamilyNameForColumn(table.getMultitenancyColumn());
                    String qualifName = HBaseUtils.getQualifierNameForColumn(table.getMultitenancyColumn());
                    put.addColumn(familyName.getBytes(), qualifName.getBytes(),
                        storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID).getBytes());
                }
            }

            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null)
            {
                Object nextVersion = VersionHelper.getNextVersion(vermd.getVersionStrategy(), null);
                op.setTransactionalVersion(nextVersion);
                if (NucleusLogger.DATASTORE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE.debug(Localiser.msg("HBase.Insert.ObjectPersistedWithVersion",
                        op.getObjectAsPrintable(), op.getInternalObjectId(), "" + nextVersion));
                }

                if (vermd.getFieldName() != null)
                {
                    // Version stored in field, so update the field
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    Object verFieldValue = nextVersion;
                    if (verMmd.getType() == int.class || verMmd.getType() == Integer.class)
                    {
                        verFieldValue = Integer.valueOf(((Long)nextVersion).intValue());
                    }
                    op.replaceField(verMmd.getAbsoluteFieldNumber(), verFieldValue);
                }
                else
                {
                    // Surrogate version, so add to the put
                    String familyName = HBaseUtils.getFamilyNameForColumn(table.getVersionColumn());
                    String qualifName = HBaseUtils.getQualifierNameForColumn(table.getVersionColumn());
                    if (nextVersion instanceof Long)
                    {
                        put.addColumn(familyName.getBytes(), qualifName.getBytes(), Bytes.toBytes((Long)nextVersion));
                    }
                    else if (nextVersion instanceof Integer)
                    {
                        put.addColumn(familyName.getBytes(), qualifName.getBytes(), Bytes.toBytes((Integer)nextVersion));
                    }
                    else
                    {
                        try
                        {
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(nextVersion);
                            put.addColumn(familyName.getBytes(), qualifName.getBytes(), bos.toByteArray());
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

            StoreFieldManager fm = new StoreFieldManager(op, put, delete, true, table);
            op.provideFields(cmd.getAllMemberPositions(), fm);

            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("HBase.Put",
                    StringUtils.toJVMIDString(op.getObject()), tableName, put));
            }
            htable.put(put);
            if (ec.getStatistics() != null)
            {
                // Add to statistics
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementInsertCount();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("HBase.ExecutionTime", (System.currentTimeMillis() - startTime)));
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
                StringBuilder fieldStr = new StringBuilder();
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        fieldStr.append(",");
                    }
                    fieldStr.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("HBase.Update.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId(), fieldStr.toString()));
            }

            if (!storeMgr.managesClass(op.getClassMetaData().getFullClassName()))
            {
                storeMgr.manageClasses(op.getExecutionContext().getClassLoaderResolver(), op.getClassMetaData().getFullClassName());
            }
            Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();

            String tableName = table.getName();
            org.apache.hadoop.hbase.client.Table htable = mconn.getHTable(tableName);
            if (cmd.isVersioned())
            {
                // Optimistic checking of version
                Result result = HBaseUtils.getResultForObject(op, htable);
                Object datastoreVersion = HBaseUtils.getVersionForObject(cmd, result, ec, table, storeMgr);
                VersionHelper.performVersionCheck(op, datastoreVersion, cmd.getVersionMetaDataForClass());
            }

            int[] updatedFieldNums = fieldNumbers;
            Put put = HBaseUtils.getPutForObject(op);
            Delete delete = HBaseUtils.getDeleteForObject(op);
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null)
            {
                // Version object so calculate version to store with
                Object nextVersion = VersionHelper.getNextVersion(vermd.getVersionStrategy(), op.getTransactionalVersion());
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
                    String familyName = HBaseUtils.getFamilyNameForColumn(table.getVersionColumn());
                    String qualifName = HBaseUtils.getQualifierNameForColumn(table.getVersionColumn());
                    if (nextVersion instanceof Long)
                    {
                        put.addColumn(familyName.getBytes(), qualifName.getBytes(), Bytes.toBytes((Long)nextVersion));
                    }
                    else if (nextVersion instanceof Integer)
                    {
                        put.addColumn(familyName.getBytes(), qualifName.getBytes(), Bytes.toBytes((Integer)nextVersion));
                    }
                    else
                    {
                        try
                        {
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(nextVersion);
                            put.addColumn(familyName.getBytes(), qualifName.getBytes(), bos.toByteArray());
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

            StoreFieldManager fm = new StoreFieldManager(op, put, delete, false, table);
            op.provideFields(updatedFieldNums, fm);
            if (!put.isEmpty())
            {
                if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("HBase.Put", StringUtils.toJVMIDString(op.getObject()), tableName, put));
                }
                htable.put(put);
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
                    NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("HBase.Delete", StringUtils.toJVMIDString(op.getObject()), tableName, delete));
                }
                htable.delete(delete);
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
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("HBase.ExecutionTime", (System.currentTimeMillis() - startTime)));
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

        ExecutionContext ec = ops[0].getExecutionContext();

        // Separate the objects to be deleted into groups, for the "table" in question
        Map<String, Set<ObjectProvider>> opsByTable = new HashMap<String, Set<ObjectProvider>>();
        for (int i=0;i<ops.length;i++)
        {
            AbstractClassMetaData cmd = ops[i].getClassMetaData();
            Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();
            String tableName = table.getName();
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
            HBaseManagedConnection mconn = (HBaseManagedConnection)storeMgr.getConnection(ec);
            try
            {
                long startTime = System.currentTimeMillis();
                org.apache.hadoop.hbase.client.Table htable = mconn.getHTable(tableName);
                Table table = ec.getStoreManager().getStoreDataForClass(opsForTable.iterator().next().getClassMetaData().getFullClassName()).getTable();

                List<Delete> deletes = new ArrayList(opsForTable.size());
                for (ObjectProvider op : opsForTable)
                {
                    // Check if read-only so update not permitted
                    assertReadOnlyForUpdateOfObject(op);

                    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("HBase.Delete.Start", op.getObjectAsPrintable(), op.getInternalObjectId()));
                    }
                    boolean deletable = true;
                    AbstractClassMetaData cmd = op.getClassMetaData();
                    if (cmd.isVersioned())
                    {
                        // Optimistic checking of version
                        Object currentVersion = op.getTransactionalVersion();
                        Result result = HBaseUtils.getResultForObject(op, htable);
                        Object datastoreVersion = HBaseUtils.getVersionForObject(cmd, result, ec, table, storeMgr);
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
                htable.delete(deletes);
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
                    NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("HBase.ExecutionTime", (System.currentTimeMillis() - startTime)));
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
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("HBase.Delete.Start", op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();
            String tableName = table.getName();
            org.apache.hadoop.hbase.client.Table htable = mconn.getHTable(tableName);
            if (cmd.isVersioned())
            {
                // Optimistic checking of version
                Result result = HBaseUtils.getResultForObject(op, htable);
                Object datastoreVersion = HBaseUtils.getVersionForObject(cmd, result, ec, table, storeMgr);
                VersionHelper.performVersionCheck(op, datastoreVersion, cmd.getVersionMetaDataForClass());
            }

            // Invoke any cascade deletion
            op.loadUnloadedFields();
            op.provideFields(cmd.getAllMemberPositions(), new DeleteFieldManager(op));

            // Delete the object
            Delete delete = HBaseUtils.getDeleteForObject(op);
            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("HBase.Delete", StringUtils.toJVMIDString(op.getObject()), tableName, delete));
            }
            htable.delete(delete);
            if (ec.getStatistics() != null)
            {
                // Add to statistics
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementDeleteCount();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("HBase.ExecutionTime", (System.currentTimeMillis() - startTime)));
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
                StringBuilder str = new StringBuilder("Fetching object \"");
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
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("HBase.Fetch.Start", op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();
            String tableName = table.getName();
            org.apache.hadoop.hbase.client.Table htable = mconn.getHTable(tableName);
            Result result = HBaseUtils.getResultForObject(op, htable);
            if (result.getRow() == null)
            {
                throw new NucleusObjectNotFoundException();
            }
            else if (cmd.hasDiscriminatorStrategy())
            {
                String familyName = HBaseUtils.getFamilyNameForColumn(table.getDiscriminatorColumn());
                String columnName = HBaseUtils.getQualifierNameForColumn(table.getDiscriminatorColumn());
                Object discValue = new String(result.getValue(familyName.getBytes(), columnName.getBytes()));
                if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.CLASS_NAME && !cmd.getFullClassName().equals(discValue))
                {
                    throw new NucleusObjectNotFoundException();
                }
                else if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.VALUE_MAP && !cmd.getDiscriminatorValue().equals(discValue))
                {
                    throw new NucleusObjectNotFoundException();
                }
            }

            Set<Integer> persistableFields = new HashSet<Integer>();
            for (int i=0;i<fieldNumbers.length;i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
                if (mmd.getPersistenceModifier() == FieldPersistenceModifier.PERSISTENT)
                {
                    persistableFields.add(fieldNumbers[i]);
                }
                else
                {
                    op.replaceField(fieldNumbers[i], op.provideField(fieldNumbers[i]));
                }
            }
            int[] reqdFieldNumbers = new int[persistableFields.size()];
            int i = 0;
            for (Integer fldNo : persistableFields)
            {
                reqdFieldNumbers[i++] = fldNo;
            }

            FetchFieldManager fm = new FetchFieldManager(op, result, table);
            op.replaceFields(reqdFieldNumbers, fm);

            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null && op.getTransactionalVersion() == null)
            {
                // No version set, so retrieve it
                if (vermd.getFieldName() != null)
                {
                    // Version stored in a field
                    Object datastoreVersion = op.provideField(cmd.getAbsolutePositionOfMember(vermd.getFieldName()));
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
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("HBase.ExecutionTime", (System.currentTimeMillis() - startTime)));
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
        ExecutionContext ec = op.getExecutionContext();
        try
        {
            final AbstractClassMetaData cmd = op.getClassMetaData();
            Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();
            String tableName = table.getName();
            org.apache.hadoop.hbase.client.Table htable = mconn.getHTable(tableName);
            if (!HBaseUtils.objectExistsInTable(op, htable))
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
            Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();
            if (!cmd.pkIsDatastoreAttributed(storeMgr))
            {
                String tableName = table.getName();
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