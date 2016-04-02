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
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
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
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.NucleusLogger;

class HBaseQueryUtils
{
    private HBaseQueryUtils() {}

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
            Class candidateClass, boolean subclasses, boolean ignoreCache, FetchPlan fetchPlan, Filter filter, StoreManager storeMgr)
    {
        List<AbstractClassMetaData> cmds = MetaDataUtils.getMetaDataForCandidates(candidateClass, subclasses, ec);
        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_NATIVE.debug("Retrieving objects for candidate=" + candidateClass.getName() +
                (subclasses ? " and subclasses" : "") + (filter != null ? (" with filter=" + filter) : ""));
        }

        List results = new ArrayList();
        for (AbstractClassMetaData cmd : cmds)
        {
            if (cmd instanceof ClassMetaData && ((ClassMetaData)cmd).isAbstract())
            {
                // Omit any classes that are not instantiable (e.g abstract)
            }
            else
            {
                results.addAll(getObjectsOfType(ec, mconn, cmd, ignoreCache, fetchPlan, filter, storeMgr));
            }
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
            final AbstractClassMetaData cmd, boolean ignoreCache, FetchPlan fp, final Filter filter, final StoreManager storeMgr)
    {
        List results = new ArrayList();

        if (!storeMgr.managesClass(cmd.getFullClassName()))
        {
            storeMgr.manageClasses(ec.getClassLoaderResolver(), cmd.getFullClassName());
        }
        final Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();
        final String tableName = table.getName();
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
                        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
                        {
                            if (RelationType.isRelationSingleValued(relationType))
                            {
                                // 1-1 embedded
                                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
                                embMmds.add(mmd);
                                addColumnsToScanForEmbeddedMember(scan, embMmds, table, ec);
                            }
                        }
                        else
                        {
                            Column col = table.getMemberColumnMappingForMember(mmd).getColumn(0); // TODO Support multicol mapping
                            byte[] familyName = HBaseUtils.getFamilyNameForColumn(col).getBytes();
                            byte[] qualifName = HBaseUtils.getQualifierNameForColumn(col).getBytes();
                            scan.addColumn(familyName, qualifName);
                        }
                    }

                    VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                    if (cmd.isVersioned() && vermd.getFieldName() == null)
                    {
                        // Add version column
                        byte[] familyName = HBaseUtils.getFamilyNameForColumn(table.getVersionColumn()).getBytes();
                        byte[] qualifName = HBaseUtils.getQualifierNameForColumn(table.getVersionColumn()).getBytes();
                        scan.addColumn(familyName, qualifName);
                    }
                    if (cmd.hasDiscriminatorStrategy())
                    {
                        // Add discriminator column
                        byte[] familyName = HBaseUtils.getFamilyNameForColumn(table.getDiscriminatorColumn()).getBytes();
                        byte[] qualifName = HBaseUtils.getQualifierNameForColumn(table.getDiscriminatorColumn()).getBytes();
                        scan.addColumn(familyName, qualifName);
                    }
                    if (cmd.getIdentityType() == IdentityType.DATASTORE)
                    {
                        // Add datastore identity column
                        byte[] familyName = HBaseUtils.getFamilyNameForColumn(table.getDatastoreIdColumn()).getBytes();
                        byte[] qualifName = HBaseUtils.getQualifierNameForColumn(table.getDatastoreIdColumn()).getBytes();
                        scan.addColumn(familyName, qualifName);
                    }

                    org.apache.hadoop.hbase.client.Table htable = mconn.getHTable(tableName);
                    ResultScanner scanner = htable.getScanner(scan);
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
                    Object obj = getObjectUsingApplicationIdForResult(result, cmd, ec, ignoreCache, fpMembers, tableName, storeMgr, table);
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
                    Object obj = getObjectUsingDatastoreIdForResult(result, cmd, ec, ignoreCache, fpMembers, tableName, storeMgr, table);
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
                    Object obj = getObjectUsingNondurableIdForResult(result, cmd, ec, ignoreCache, fpMembers, tableName, storeMgr, table);
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
            final ExecutionContext ec, boolean ignoreCache, final int[] fpMembers, String tableName, StoreManager storeMgr, Table table)
    {
        if (cmd.hasDiscriminatorStrategy())
        {
            // Check the class for this discriminator value
            String familyName = HBaseUtils.getFamilyNameForColumn(table.getDiscriminatorColumn());
            String columnName = HBaseUtils.getQualifierNameForColumn(table.getDiscriminatorColumn());
            Object discValue = new String(result.getValue(familyName.getBytes(), columnName.getBytes()));
            if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.CLASS_NAME && !cmd.getFullClassName().equals(discValue))
            {
                return null;
            }
            else if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.VALUE_MAP && !cmd.getDiscriminatorValue().equals(discValue))
            {
                return null;
            }
        }

        final FieldManager fm = new FetchFieldManager(ec, cmd, result, table);
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
            final ExecutionContext ec, boolean ignoreCache, final int[] fpMembers, String tableName, StoreManager storeMgr, Table table)
    {
        if (cmd.hasDiscriminatorStrategy())
        {
            // Check the class for this discriminator value
            String familyName = HBaseUtils.getFamilyNameForColumn(table.getDiscriminatorColumn());
            String columnName = HBaseUtils.getQualifierNameForColumn(table.getDiscriminatorColumn());
            Object discValue = new String(result.getValue(familyName.getBytes(), columnName.getBytes()));
            if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.CLASS_NAME && !cmd.getFullClassName().equals(discValue))
            {
                return null;
            }
            else if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.VALUE_MAP && !cmd.getDiscriminatorValue().equals(discValue))
            {
                return null;
            }
        }

        String dsidFamilyName = HBaseUtils.getFamilyNameForColumn(table.getDatastoreIdColumn());
        String dsidColumnName = HBaseUtils.getQualifierNameForColumn(table.getDatastoreIdColumn());
        Object id = null;
        try
        {
            byte[] bytes = result.getValue(dsidFamilyName.getBytes(), dsidColumnName.getBytes());
            if (bytes != null)
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                Object key = ois.readObject();
                id = ec.getNucleusContext().getIdentityManager().getDatastoreId(cmd.getFullClassName(), key);
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

        final FieldManager fm = new FetchFieldManager(ec, cmd, result, table);
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
            ObjectProvider op = ec.findObjectProvider(pc);
            Object version = null;
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getFieldName() != null)
            {
                // Set the version from the field value
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                version = op.provideField(verMmd.getAbsoluteFieldNumber());
            }
            else
            {
                // Get the surrogate version from the datastore
                version = HBaseUtils.getSurrogateVersionForObject(cmd, result, tableName, storeMgr);
            }
            op.setVersion(version);
        }

        if (result.getRow() != null)
        {
            ObjectProvider sm = ec.findObjectProvider(pc);
            sm.setAssociatedValue("HBASE_ROW_KEY", result.getRow());
        }

        return pc;
    }

    protected static Object getObjectUsingNondurableIdForResult(final Result result, final AbstractClassMetaData cmd,
            final ExecutionContext ec, boolean ignoreCache, final int[] fpMembers, String tableName, StoreManager storeMgr, Table table)
    {
        if (cmd.hasDiscriminatorStrategy())
        {
            // Check the class for this discriminator value
            String familyName = HBaseUtils.getFamilyNameForColumn(table.getDiscriminatorColumn());
            String columnName = HBaseUtils.getQualifierNameForColumn(table.getDiscriminatorColumn());
            Object discValue = new String(result.getValue(familyName.getBytes(), columnName.getBytes()));
            if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.CLASS_NAME && !cmd.getFullClassName().equals(discValue))
            {
                return null;
            }
            else if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.VALUE_MAP && !cmd.getDiscriminatorValue().equals(discValue))
            {
                return null;
            }
        }

        final FieldManager fm = new FetchFieldManager(ec, cmd, result, table);
        SCOID id = new SCOID(cmd.getFullClassName());
        Class type = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
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
            }, type, ignoreCache, false);

        if (cmd.isVersioned())
        {
            // Set the version on the object
            ObjectProvider op = ec.findObjectProvider(pc);
            Object version = null;
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getFieldName() != null)
            {
                // Set the version from the field value
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                version = op.provideField(verMmd.getAbsoluteFieldNumber());
            }
            else
            {
                // Get the surrogate version from the datastore
                version = HBaseUtils.getSurrogateVersionForObject(cmd, result, tableName, storeMgr);
            }
            op.setVersion(version);
        }

        if (result.getRow() != null)
        {
            ObjectProvider sm = ec.findObjectProvider(pc);
            sm.setAssociatedValue("HBASE_ROW_KEY", result.getRow());
        }

        return pc;
    }

    private static void addColumnsToScanForEmbeddedMember(Scan scan, List<AbstractMemberMetaData> embMmds, Table table, ExecutionContext ec)
    {
        AbstractMemberMetaData lastMmd = embMmds.get(embMmds.size()-1);
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(lastMmd.getTypeName(), clr);
        int[] embMmdPosns = embCmd.getAllMemberPositions();
        for (int i=0;i<embMmdPosns.length;i++)
        {
            AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(i);
            List<AbstractMemberMetaData> subEmbMmds = new ArrayList<AbstractMemberMetaData>(embMmds);
            subEmbMmds.add(embMmd);
            RelationType relationType = embMmd.getRelationType(clr);
            MemberColumnMapping mapping = table.getMemberColumnMappingForEmbeddedMember(subEmbMmds);
            if (RelationType.isRelationSingleValued(relationType))
            {
                // TODO Catch bidirectional relations
                addColumnsToScanForEmbeddedMember(scan, subEmbMmds, table, ec);
            }
            else
            {
                String familyName = HBaseUtils.getFamilyNameForColumn(mapping.getColumn(0));
                String qualifName = HBaseUtils.getQualifierNameForColumn(mapping.getColumn(0));
                scan.addColumn(familyName.getBytes(), qualifName.getBytes());
            }
        }
    }
}