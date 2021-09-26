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
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.fieldmanager.DeleteFieldManager;
import org.datanucleus.store.hbase.fieldmanager.FetchFieldManager;
import org.datanucleus.store.hbase.fieldmanager.StoreFieldManager;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.SurrogateColumnType;
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
     * @see org.datanucleus.store.AbstractPersistenceHandler#insertObjects(org.datanucleus.state.DNStateManager[])
     */
    @Override
    public void insertObjects(DNStateManager... sms)
    {
        if (sms.length == 1)
        {
            insertObject(sms[0]);
            return;
        }

        // TODO Implement any bulk insert
        super.insertObjects(sms);
    }

    public void insertObject(DNStateManager sm)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(sm);

        ExecutionContext ec = sm.getExecutionContext();
        AbstractClassMetaData cmd = sm.getClassMetaData();
        StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
        if (sd == null)
        {
            storeMgr.manageClasses(ec.getClassLoaderResolver(), new String[]{cmd.getFullClassName()});
            sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
        }
        Table table = sd.getTable();

        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnectionManager().getConnection(ec);
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
                        if (HBaseUtils.objectExistsInTable(sm, htable, table))
                        {
                            throw new NucleusUserException(Localiser.msg("HBase.Insert.ObjectWithIdAlreadyExists", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
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
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("HBase.Insert.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }

            Put put = HBaseUtils.getPutForObject(sm, table);
            Delete delete = HBaseUtils.getDeleteForObject(sm, table);

            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                String familyName = HBaseUtils.getFamilyNameForColumn(table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID));
                String qualifName = HBaseUtils.getQualifierNameForColumn(table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID));
                Object key = IdentityUtils.getTargetKeyForDatastoreIdentity(sm.getInternalObjectId());
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
                // Discriminator field
                String discVal = (String)cmd.getDiscriminatorValue(); // TODO Allow non-String
                String familyName = HBaseUtils.getFamilyNameForColumn(table.getSurrogateColumn(SurrogateColumnType.DISCRIMINATOR));
                String qualifName = HBaseUtils.getQualifierNameForColumn(table.getSurrogateColumn(SurrogateColumnType.DISCRIMINATOR));

                put.addColumn(familyName.getBytes(), qualifName.getBytes(), discVal.getBytes());
            }

            Column multitenancyCol = table.getSurrogateColumn(SurrogateColumnType.MULTITENANCY);
            if (multitenancyCol != null)
            {
                String tenantId = ec.getTenantId();
                if (tenantId != null)
                {
                    // Multi-tenancy discriminator
                    String familyName = HBaseUtils.getFamilyNameForColumn(multitenancyCol);
                    String qualifName = HBaseUtils.getQualifierNameForColumn(multitenancyCol);
                    put.addColumn(familyName.getBytes(), qualifName.getBytes(), tenantId.getBytes());
                }
            }

            Column softDeleteCol = table.getSurrogateColumn(SurrogateColumnType.SOFTDELETE);
            if (softDeleteCol != null)
            {
                // Soft-delete flag
                String familyName = HBaseUtils.getFamilyNameForColumn(softDeleteCol);
                String qualifName = HBaseUtils.getQualifierNameForColumn(softDeleteCol);
                put.addColumn(familyName.getBytes(), qualifName.getBytes(), Bytes.toBytes(Boolean.FALSE));
            }

            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null)
            {
                Object nextVersion = ec.getLockManager().getNextVersion(vermd, null);
                sm.setTransactionalVersion(nextVersion);
                if (NucleusLogger.DATASTORE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE.debug(Localiser.msg("HBase.Insert.ObjectPersistedWithVersion", sm.getObjectAsPrintable(), sm.getInternalObjectId(), "" + nextVersion));
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
                    sm.replaceField(verMmd.getAbsoluteFieldNumber(), verFieldValue);
                }
                else
                {
                    // Surrogate version, so add to the put
                    String familyName = HBaseUtils.getFamilyNameForColumn(table.getSurrogateColumn(SurrogateColumnType.VERSION));
                    String qualifName = HBaseUtils.getQualifierNameForColumn(table.getSurrogateColumn(SurrogateColumnType.VERSION));
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

            StoreFieldManager fm = new StoreFieldManager(sm, put, delete, true, table);
            sm.provideFields(cmd.getAllMemberPositions(), fm);

            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("HBase.Put", StringUtils.toJVMIDString(sm.getObject()), tableName, put.toString(50)));
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

    public void updateObject(DNStateManager sm, int[] fieldNumbers)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(sm);

        ExecutionContext ec = sm.getExecutionContext();
        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            long startTime = System.currentTimeMillis();
            AbstractClassMetaData cmd = sm.getClassMetaData();
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
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("HBase.Update.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId(), fieldStr.toString()));
            }

            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                storeMgr.manageClasses(ec.getClassLoaderResolver(), new String[]{cmd.getFullClassName()});
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();

            String tableName = table.getName();
            org.apache.hadoop.hbase.client.Table htable = mconn.getHTable(tableName);
            if (cmd.isVersioned())
            {
                // Optimistic checking of version
                Result result = HBaseUtils.getResultForObject(sm, htable, table);
                Object datastoreVersion = HBaseUtils.getVersionForObject(cmd, result, ec, table, storeMgr);
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                ec.getLockManager().performOptimisticVersionCheck(sm, vermd!=null ? vermd.getVersionStrategy() : null, datastoreVersion);
            }

            int[] updatedFieldNums = fieldNumbers;
            Put put = HBaseUtils.getPutForObject(sm, table);
            Delete delete = HBaseUtils.getDeleteForObject(sm, table);
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null)
            {
                // Version object so calculate version to store with
                Object nextVersion = ec.getLockManager().getNextVersion(vermd, sm.getTransactionalVersion());
                sm.setTransactionalVersion(nextVersion);

                if (vermd.getFieldName() != null)
                {
                    // Update the field version value
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    sm.replaceField(verMmd.getAbsoluteFieldNumber(), nextVersion);
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
                    String familyName = HBaseUtils.getFamilyNameForColumn(table.getSurrogateColumn(SurrogateColumnType.VERSION));
                    String qualifName = HBaseUtils.getQualifierNameForColumn(table.getSurrogateColumn(SurrogateColumnType.VERSION));
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

            StoreFieldManager fm = new StoreFieldManager(sm, put, delete, false, table);
            sm.provideFields(updatedFieldNums, fm);
            if (!put.isEmpty())
            {
                if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("HBase.Put", StringUtils.toJVMIDString(sm.getObject()), tableName, put));
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
                    NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("HBase.Delete", StringUtils.toJVMIDString(sm.getObject()), tableName, delete));
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
     * @see org.datanucleus.store.AbstractPersistenceHandler#deleteObjects(org.datanucleus.state.DNStateManager[])
     */
    @Override
    public void deleteObjects(DNStateManager... sms)
    {
        if (sms.length == 1)
        {
            deleteObject(sms[0]);
            return;
        }

        ExecutionContext ec = sms[0].getExecutionContext();

        // Separate the objects to be deleted into groups, for the "table" in question
        Map<String, Set<DNStateManager>> smsByTable = new HashMap<String, Set<DNStateManager>>();
        for (int i=0;i<sms.length;i++)
        {
            AbstractClassMetaData cmd = sms[i].getClassMetaData();
            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                storeMgr.manageClasses(ec.getClassLoaderResolver(), new String[]{cmd.getFullClassName()});
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();
            String tableName = table.getName();
            Set<DNStateManager> smsForTable = smsByTable.get(tableName);
            if (smsForTable == null)
            {
                smsForTable = new HashSet<DNStateManager>();
                smsByTable.put(tableName, smsForTable);
            }
            smsForTable.add(sms[i]);
        }

        Set<NucleusOptimisticException> optimisticExcps = null;
        for (Map.Entry<String, Set<DNStateManager>> entry : smsByTable.entrySet())
        {
            String tableName = entry.getKey();
            Set<DNStateManager> smsForTable = entry.getValue();
            HBaseManagedConnection mconn = (HBaseManagedConnection)storeMgr.getConnectionManager().getConnection(ec);
            try
            {
                long startTime = System.currentTimeMillis();
                org.apache.hadoop.hbase.client.Table htable = mconn.getHTable(tableName);
                Table table = storeMgr.getStoreDataForClass(smsForTable.iterator().next().getClassMetaData().getFullClassName()).getTable();

                List<Delete> deletes = new ArrayList(smsForTable.size());
                for (DNStateManager sm : smsForTable)
                {
                    // Check if read-only so update not permitted
                    assertReadOnlyForUpdateOfObject(sm);

                    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("HBase.Delete.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
                    }
                    boolean deletable = true;
                    AbstractClassMetaData cmd = sm.getClassMetaData();
                    if (cmd.isVersioned())
                    {
                        // Optimistic checking of version
                        Object currentVersion = sm.getTransactionalVersion();
                        Result result = HBaseUtils.getResultForObject(sm, htable, table);
                        Object datastoreVersion = HBaseUtils.getVersionForObject(cmd, result, ec, table, storeMgr);
                        if (!datastoreVersion.equals(currentVersion)) // TODO Use VersionHelper.performVersionCheck
                        {
                            if (optimisticExcps == null)
                            {
                                optimisticExcps = new HashSet<NucleusOptimisticException>();
                            }
                            optimisticExcps.add(new NucleusOptimisticException("Cannot delete object with id " + 
                                sm.getInternalObjectId() + " since has version=" + currentVersion + " while datastore has version=" + datastoreVersion));
                            deletable = false;
                        }
                    }

                    if (deletable)
                    {
                        // Invoke any cascade deletion
                        sm.loadUnloadedFields();
                        sm.provideFields(cmd.getAllMemberPositions(), new DeleteFieldManager(sm, true));

                        // Delete the object
                        deletes.add(HBaseUtils.getDeleteForObject(sm, table));
                    }
                }

                // Delete all rows from this table
                htable.delete(deletes);
                if (ec.getStatistics() != null)
                {
                    // Add to statistics
                    ec.getStatistics().incrementNumWrites();
                    for (int i=0;i<smsForTable.size();i++)
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

    public void deleteObject(DNStateManager sm)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(sm);

        ExecutionContext ec = sm.getExecutionContext();
        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            AbstractClassMetaData cmd = sm.getClassMetaData();
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("HBase.Delete.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }

            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                storeMgr.manageClasses(ec.getClassLoaderResolver(), new String[]{cmd.getFullClassName()});
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();
            String tableName = table.getName();
            org.apache.hadoop.hbase.client.Table htable = mconn.getHTable(tableName);
            if (cmd.isVersioned())
            {
                // Optimistic checking of version
                Result result = HBaseUtils.getResultForObject(sm, htable, table);
                Object datastoreVersion = HBaseUtils.getVersionForObject(cmd, result, ec, table, storeMgr);
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                ec.getLockManager().performOptimisticVersionCheck(sm, vermd!=null ? vermd.getVersionStrategy() : null, datastoreVersion);
            }

            // Invoke any cascade deletion
            sm.loadUnloadedFields();
            sm.provideFields(cmd.getAllMemberPositions(), new DeleteFieldManager(sm, true));

            // Delete the object
            // TODO Support SOFT_DELETE
            Delete delete = HBaseUtils.getDeleteForObject(sm, table);
            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug(Localiser.msg("HBase.Delete", StringUtils.toJVMIDString(sm.getObject()), tableName, delete));
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

    public void fetchObject(DNStateManager sm, int[] fieldNumbers)
    {
        ExecutionContext ec = sm.getExecutionContext();
        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            AbstractClassMetaData cmd = sm.getClassMetaData();
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                // Debug information about what we are retrieving
                StringBuilder str = new StringBuilder("Fetching object \"");
                str.append(sm.getObjectAsPrintable()).append("\" (id=");
                str.append(sm.getInternalObjectId()).append(")").append(" fields [");
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
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("HBase.Fetch.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }

            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                storeMgr.manageClasses(ec.getClassLoaderResolver(), new String[]{cmd.getFullClassName()});
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();
            String tableName = table.getName();
            org.apache.hadoop.hbase.client.Table htable = mconn.getHTable(tableName);
            Result result = HBaseUtils.getResultForObject(sm, htable, table);
            if (result.getRow() == null)
            {
                throw new NucleusObjectNotFoundException();
            }
            else if (cmd.hasDiscriminatorStrategy())
            {
                String familyName = HBaseUtils.getFamilyNameForColumn(table.getSurrogateColumn(SurrogateColumnType.DISCRIMINATOR));
                String columnName = HBaseUtils.getQualifierNameForColumn(table.getSurrogateColumn(SurrogateColumnType.DISCRIMINATOR));
                Object discValue = new String(result.getValue(familyName.getBytes(), columnName.getBytes()));
                Object cmdDiscValue = cmd.getDiscriminatorValue();
                if (cmd.getDiscriminatorStrategy() != DiscriminatorStrategy.NONE && !cmdDiscValue.equals(discValue))
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
                    sm.replaceField(fieldNumbers[i], sm.provideField(fieldNumbers[i]));
                }
            }
            int[] reqdFieldNumbers = new int[persistableFields.size()];
            int i = 0;
            for (Integer fldNo : persistableFields)
            {
                reqdFieldNumbers[i++] = fldNo;
            }

            FetchFieldManager fm = new FetchFieldManager(sm, result, table);
            sm.replaceFields(reqdFieldNumbers, fm);

            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null && sm.getTransactionalVersion() == null)
            {
                // No version set, so retrieve it
                if (vermd.getFieldName() != null)
                {
                    // Version stored in a field
                    Object datastoreVersion = sm.provideField(cmd.getAbsolutePositionOfMember(vermd.getFieldName()));
                    sm.setVersion(datastoreVersion);
                }
                else
                {
                    // Surrogate version
                    sm.setVersion(HBaseUtils.getSurrogateVersionForObject(cmd, result, tableName, storeMgr));
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

    public void locateObject(DNStateManager sm)
    {
        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnectionManager().getConnection(sm.getExecutionContext());
        ExecutionContext ec = sm.getExecutionContext();
        try
        {
            final AbstractClassMetaData cmd = sm.getClassMetaData();
            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                storeMgr.manageClasses(ec.getClassLoaderResolver(), new String[]{cmd.getFullClassName()});
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();
            String tableName = table.getName();
            org.apache.hadoop.hbase.client.Table htable = mconn.getHTable(tableName);
            if (!HBaseUtils.objectExistsInTable(sm, htable, table))
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
     * @see org.datanucleus.store.AbstractPersistenceHandler#locateObjects(org.datanucleus.state.DNStateManager[])
     */
    @Override
    public void locateObjects(DNStateManager[] sms)
    {
        // TODO This requires HBase 0.95 which adds exists(List) method.
        /*// Separate the objects to be persisted into groups, for the "table" in question
        Map<String, Set<DNStateManager>> smsByTable = new HashMap();
        for (int i=0;i<sms.length;i++)
        {
            AbstractClassMetaData cmd = sms[i].getClassMetaData();
            Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();
            if (!cmd.pkIsDatastoreAttributed(storeMgr))
            {
                String tableName = table.getName();
                Set<DNStateManager> smsForTable = smsByTable.get(tableName);
                if (smsForTable == null)
                {
                    smsForTable = new HashSet<DNStateManager>();
                    smsByTable.put(tableName, smsForTable);
                }
                smsForTable.add(sms[i]);
            }
        }

        ExecutionContext ec = sms[0].getExecutionContext();
        for (String tableName : smsByTable.keySet())
        {
            Set<DNStateManager> smsForTable = smsByTable.get(tableName);
            HBaseManagedConnection mconn = (HBaseManagedConnection)storeMgr.getConnection(ec);
            try
            {
                HTableInterface table = mconn.getHTable(tableName);
                List<Get> gets = new ArrayList<Get>(opsForTable.size());
                for (DNStateManager sm : smsForTable)
                {
                    Get get = HBaseUtils.getGetForObject(sm);
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
        super.locateObjects(sms);
    }
}