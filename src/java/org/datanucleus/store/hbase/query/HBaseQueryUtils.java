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
package org.datanucleus.store.hbase.query;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.OID;
import org.datanucleus.identity.OIDFactory;
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.hbase.HBaseManagedConnection;
import org.datanucleus.store.hbase.HBaseUtils;
import org.datanucleus.store.hbase.fieldmanager.FetchFieldManager;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.util.NucleusLogger;

class HBaseQueryUtils
{
    /**
     * Convenience method to get all objects of the candidate type (and optional subclasses) from the 
     * specified connection.
     * @param ec Execution Context
     * @param mconn Managed Connection
     * @param candidateClass Candidate
     * @param subclasses Include subclasses?
     * @param ignoreCache Whether to ignore the cache
     * @param fetchPlan Fetch Plan
     * @param filter Optional filter for the candidates
     * @return List of objects of the candidate type (or subclass)
     */
    static List getObjectsOfCandidateType(final ExecutionContext ec, final HBaseManagedConnection mconn,
            Class candidateClass, boolean subclasses, boolean ignoreCache, FetchPlan fetchPlan, Filter filter,
            StoreManager storeMgr)
    {
        List<AbstractClassMetaData> cmds = 
            MetaDataUtils.getMetaDataForCandidates(candidateClass, subclasses, ec);

        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_NATIVE.debug("Retrieving objects for candidate=" + candidateClass.getName() +
                (subclasses ? " and subclasses" : "") +
                (filter != null ? (" with filter=" + filter) : ""));
        }
        Iterator<AbstractClassMetaData> cmdIter = cmds.iterator();
        List results = new ArrayList();
        while (cmdIter.hasNext())
        {
            AbstractClassMetaData acmd = cmdIter.next();
            results.addAll(getObjectsOfType(ec, mconn, acmd, ignoreCache, fetchPlan, filter, storeMgr));
        }

        return results;
    }

    /**
     * Convenience method to get all objects of the specified type.
     * @param ec Execution Context
     * @param mconn Managed Connection
     * @param cmd Metadata for the type to return
     * @param ignoreCache Whether to ignore the cache
     * @param fp Fetch Plan
     * @param filter Optional filter for the candidates
     * @param storeMgr StoreManager in use
     * @return List of objects of the candidate type
     */
    static private List getObjectsOfType(final ExecutionContext ec, final HBaseManagedConnection mconn,
            final AbstractClassMetaData cmd, boolean ignoreCache, FetchPlan fp, final Filter filter,
            final StoreManager storeMgr)
    {
        List results = new ArrayList();

        final String tableName = storeMgr.getNamingFactory().getTableName(cmd);
        final int[] fpMembers = fp.getFetchPlanForClass(cmd).getMemberNumbers();
        try
        {
            final ClassLoaderResolver clr = ec.getClassLoaderResolver();

            Iterator<Result> it = (Iterator<Result>) AccessController.doPrivileged(new PrivilegedExceptionAction()
            {
                public Object run() throws Exception
                {
                    Scan scan = new Scan();
                    if (filter != null)
                    {
                        scan.setFilter(filter);
                    }

                    // Retrieve all fetch-plan fields
                    for (int i=0; i<fpMembers.length; i++)
                    {
                        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fpMembers[i]);
                        RelationType relationType = mmd.getRelationType(clr);
                        if (RelationType.isRelationSingleValued(relationType) && mmd.isEmbedded())
                        {
                            addColumnsToScanForEmbeddedMember(scan, mmd, tableName, ec);
                        }
                        else
                        {
                            byte[] familyName = HBaseUtils.getFamilyName(cmd, fpMembers[i], tableName).getBytes();
                            byte[] qualifName = HBaseUtils.getQualifierName(cmd, fpMembers[i]).getBytes();
                            scan.addColumn(familyName, qualifName);
                        }
                    }

                    VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                    if (cmd.isVersioned() && vermd.getFieldName() == null)
                    {
                        // Add version column
                        String colName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.VERSION_COLUMN);
                        byte[] familyName = HBaseUtils.getFamilyNameForColumnName(colName, tableName).getBytes();
                        byte[] qualifName = HBaseUtils.getQualifierNameForColumnName(colName).getBytes();
                        scan.addColumn(familyName, qualifName);
                    }
                    if (cmd.hasDiscriminatorStrategy())
                    {
                        // Add discriminator column
                        String colName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.DISCRIMINATOR_COLUMN);
                        byte[] familyName = HBaseUtils.getFamilyNameForColumnName(colName, tableName).getBytes();
                        byte[] qualifName = HBaseUtils.getQualifierNameForColumnName(colName).getBytes();
                        scan.addColumn(familyName, qualifName);
                    }
                    if (cmd.getIdentityType() == IdentityType.DATASTORE)
                    {
                        // Add datastore identity column
                        String colName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.DATASTOREID_COLUMN);
                        byte[] familyName = HBaseUtils.getFamilyNameForColumnName(colName, tableName).getBytes();
                        byte[] qualifName = HBaseUtils.getQualifierNameForColumnName(colName).getBytes();
                        scan.addColumn(familyName, qualifName);
                    }

                    HTableInterface table = mconn.getHTable(tableName);
                    ResultScanner scanner = table.getScanner(scan);
                    if (ec.getStatistics() != null)
                    {
                        // Add to statistics
                        ec.getStatistics().incrementNumReads();
                    }
                    Iterator<Result> it = scanner.iterator();
                    return it;
                }
            });

            // Instantiate the objects
            if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                while (it.hasNext())
                {
                    final Result result = it.next();
                    Object obj = getObjectUsingApplicationIdForResult(result, cmd, ec, ignoreCache, fpMembers, tableName,
                        storeMgr);
                    if (obj != null)
                    {
                        results.add(obj);
                    }
                }
            }
            else if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                while (it.hasNext())
                {
                    final Result result = it.next();
                    Object obj = getObjectUsingDatastoreIdForResult(result, cmd, ec, ignoreCache, fpMembers, tableName,
                        storeMgr);
                    if (obj != null)
                    {
                        results.add(obj);
                    }
                }
            }
            else
            {
                while (it.hasNext())
                {
                    final Result result = it.next();
                    Object obj = getObjectUsingNondurableIdForResult(result, cmd, ec, ignoreCache, fpMembers, tableName,
                        storeMgr);
                    if (obj != null)
                    {
                        results.add(obj);
                    }
                }
            }
        }
        catch (PrivilegedActionException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e.getCause());
        }
        return results;
    }

    protected static Object getObjectUsingApplicationIdForResult(final Result result, final AbstractClassMetaData cmd,
            final ExecutionContext ec, boolean ignoreCache, final int[] fpMembers, String tableName, StoreManager storeMgr)
    {
        if (cmd.hasDiscriminatorStrategy())
        {
            // Check the class for this discriminator value
            String colName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.DISCRIMINATOR_COLUMN);
            String familyName = HBaseUtils.getFamilyNameForColumnName(colName, tableName);
            String columnName = HBaseUtils.getQualifierNameForColumnName(colName);
            Object discValue = new String(result.getValue(familyName.getBytes(), columnName.getBytes()));
            if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.CLASS_NAME)
            {
                if (!cmd.getFullClassName().equals(discValue))
                {
                    return null;
                }
            }
            else if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.VALUE_MAP)
            {
                if (!cmd.getDiscriminatorValue().equals(discValue))
                {
                    return null;
                }
            }
        }

        final FieldManager fm = new FetchFieldManager(ec, cmd, result, tableName);
        Object id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, cmd, null, false, fm);

        Object pc = ec.findObject(id, 
            new FieldValues()
            {
                public void fetchFields(ObjectProvider sm)
                {
                    sm.replaceFields(fpMembers, fm);
                }
                public void fetchNonLoadedFields(ObjectProvider sm)
                {
                    sm.replaceNonLoadedFields(fpMembers, fm);
                }
                public FetchPlan getFetchPlanForLoading()
                {
                    return null;
                }
            }, null, ignoreCache, false);

        if (cmd.isVersioned())
        {
            // Set the version on the object
            ObjectProvider sm = ec.findObjectProvider(pc);
            Object version = null;
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getFieldName() != null)
            {
                // Set the version from the field value
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                version = sm.provideField(verMmd.getAbsoluteFieldNumber());
            }
            else
            {
                // Get the surrogate version from the datastore
                version = HBaseUtils.getSurrogateVersionForObject(cmd, result, tableName, storeMgr);
            }
            sm.setVersion(version);
        }

        if (result.getRow() != null)
        {
            ObjectProvider sm = ec.findObjectProvider(pc);
            sm.setAssociatedValue("HBASE_ROW_KEY", result.getRow());
        }

        return pc;
    }

    protected static Object getObjectUsingDatastoreIdForResult(final Result result, final AbstractClassMetaData cmd,
            final ExecutionContext ec, boolean ignoreCache, final int[] fpMembers, String tableName, StoreManager storeMgr)
    {
        if (cmd.hasDiscriminatorStrategy())
        {
            // Check the class for this discriminator value
            String colName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.DISCRIMINATOR_COLUMN);
            String familyName = HBaseUtils.getFamilyNameForColumnName(colName, tableName);
            String columnName = HBaseUtils.getQualifierNameForColumnName(colName);
            Object discValue = new String(result.getValue(familyName.getBytes(), columnName.getBytes()));
            if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.CLASS_NAME)
            {
                if (!cmd.getFullClassName().equals(discValue))
                {
                    return null;
                }
            }
            else if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.VALUE_MAP)
            {
                if (!cmd.getDiscriminatorValue().equals(discValue))
                {
                    return null;
                }
            }
        }

        String dsidColName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.DATASTOREID_COLUMN);
        String dsidFamilyName = HBaseUtils.getFamilyNameForColumnName(dsidColName, tableName);
        String dsidColumnName = HBaseUtils.getQualifierNameForColumnName(dsidColName);
        OID id = null;
        try
        {
            byte[] bytes = result.getValue(dsidFamilyName.getBytes(), dsidColumnName.getBytes());
            if (bytes != null)
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                Object key = ois.readObject();
                id = OIDFactory.getInstance(ec.getNucleusContext(), cmd.getFullClassName(), key);
                ois.close();
                bis.close();
            }
            else
            {
                throw new NucleusException("Retrieved identity for family=" + dsidFamilyName + " column=" + dsidColumnName + " IS NULL");
            }
        }
        catch (Exception e)
        {
            throw new NucleusException(e.getMessage(), e);
        }

        final FieldManager fm = new FetchFieldManager(ec, cmd, result, tableName);
        Object pc = ec.findObject(id, 
            new FieldValues()
            {
                // ObjectProvider calls the fetchFields method
                public void fetchFields(ObjectProvider op)
                {
                    op.replaceFields(fpMembers, fm);
                }
                public void fetchNonLoadedFields(ObjectProvider op)
                {
                    op.replaceNonLoadedFields(fpMembers, fm);
                }
                public FetchPlan getFetchPlanForLoading()
                {
                    return null;
                }
            }, null, ignoreCache, false);

        if (cmd.isVersioned())
        {
            // Set the version on the object
            ObjectProvider sm = ec.findObjectProvider(pc);
            Object version = null;
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getFieldName() != null)
            {
                // Set the version from the field value
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                version = sm.provideField(verMmd.getAbsoluteFieldNumber());
            }
            else
            {
                // Get the surrogate version from the datastore
                version = HBaseUtils.getSurrogateVersionForObject(cmd, result, tableName, storeMgr);
            }
            sm.setVersion(version);
        }

        if (result.getRow() != null)
        {
            ObjectProvider sm = ec.findObjectProvider(pc);
            sm.setAssociatedValue("HBASE_ROW_KEY", result.getRow());
        }

        return pc;
    }

    protected static Object getObjectUsingNondurableIdForResult(final Result result, final AbstractClassMetaData cmd,
            final ExecutionContext ec, boolean ignoreCache, final int[] fpMembers, String tableName, StoreManager storeMgr)
    {
        if (cmd.hasDiscriminatorStrategy())
        {
            // Check the class for this discriminator value
            String colName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.DISCRIMINATOR_COLUMN);
            String familyName = HBaseUtils.getFamilyNameForColumnName(colName, tableName);
            String columnName = HBaseUtils.getQualifierNameForColumnName(colName);
            Object discValue = new String(result.getValue(familyName.getBytes(), columnName.getBytes()));
            if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.CLASS_NAME)
            {
                if (!cmd.getFullClassName().equals(discValue))
                {
                    return null;
                }
            }
            else if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.VALUE_MAP)
            {
                if (!cmd.getDiscriminatorValue().equals(discValue))
                {
                    return null;
                }
            }
        }

        final FieldManager fm = new FetchFieldManager(ec, cmd, result, tableName);
        SCOID id = new SCOID(cmd.getFullClassName());
        Object pc = ec.findObject(id, 
            new FieldValues()
            {
                // ObjectProvider calls the fetchFields method
                public void fetchFields(ObjectProvider op)
                {
                    op.replaceFields(fpMembers, fm);
                }
                public void fetchNonLoadedFields(ObjectProvider op)
                {
                    op.replaceNonLoadedFields(fpMembers, fm);
                }
                public FetchPlan getFetchPlanForLoading()
                {
                    return null;
                }
            }, null, ignoreCache, false);

        if (cmd.isVersioned())
        {
            // Set the version on the object
            ObjectProvider sm = ec.findObjectProvider(pc);
            Object version = null;
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getFieldName() != null)
            {
                // Set the version from the field value
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                version = sm.provideField(verMmd.getAbsoluteFieldNumber());
            }
            else
            {
                // Get the surrogate version from the datastore
                version = HBaseUtils.getSurrogateVersionForObject(cmd, result, tableName, storeMgr);
            }
            sm.setVersion(version);
        }

        if (result.getRow() != null)
        {
            ObjectProvider sm = ec.findObjectProvider(pc);
            sm.setAssociatedValue("HBASE_ROW_KEY", result.getRow());
        }

        return pc;
    }

    private static void addColumnsToScanForEmbeddedMember(Scan scan, AbstractMemberMetaData mmd, String tableName, ExecutionContext ec)
    {
        EmbeddedMetaData embmd = mmd.getEmbeddedMetaData();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData[] embmmds = embmd.getMemberMetaData();
        for (int i=0;i<embmmds.length;i++)
        {
            AbstractMemberMetaData embMmd = embmmds[i];
            RelationType relationType = embMmd.getRelationType(clr);
            if ((relationType == RelationType.ONE_TO_ONE_BI || relationType == RelationType.ONE_TO_ONE_UNI) && embMmd.isEmbedded())
            {
                addColumnsToScanForEmbeddedMember(scan, embMmd, tableName, ec);
            }
            else
            {
                byte[] familyName = HBaseUtils.getFamilyName(mmd, i, tableName).getBytes();
                byte[] qualifName = HBaseUtils.getQualifierName(mmd, i).getBytes();
                scan.addColumn(familyName, qualifName);
            }
        }
    }
}