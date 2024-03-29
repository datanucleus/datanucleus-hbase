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
2011 Andy Jefferson - add family/column getters for datastore id, version, embedded fields
2011 Andy Jefferson - extended schema creation, and added schema deletion
 ...
***********************************************************************/
package org.datanucleus.store.hbase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.NucleusContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.converters.TypeConverter;

public class HBaseUtils
{
    private HBaseUtils() {}

    /**
     * Accessor for the default value specified for the provided member.
     * If no defaultValue is provided on the column then returns null.
     * @param mmd Metadata for the member
     * @return The default value
     */
    public static String getDefaultValueForMember(AbstractMemberMetaData mmd)
    {
        ColumnMetaData[] colmds = mmd.getColumnMetaData();
        if (colmds == null || colmds.length < 1)
        {
            return null;
        }
        return colmds[0].getDefaultValue();
    }

    /**
     * Accessor for the family name for a column.
     * If the column name is of the form "family:qualifier" then returns family, otherwise returns the table name.
     * @param col Column
     * @return The family name for this column
     */
    public static String getFamilyNameForColumn(Column col)
    {
        if (col == null)
        {
            return null;
        }
        String colName = col.getName();
        if (colName != null && colName.indexOf(":") > 0)
        {
            return colName.substring(0, colName.indexOf(":"));
        }
        return col.getTable().getName();
    }

    /**
     * Accessor for the qualifier name for a column name.
     * If the column name is of the form "family:qualifier" then returns qualifier, otherwise returns the column name.
     * @param col Column
     * @return The qualifier name for this column
     */
    public static String getQualifierNameForColumn(Column col)
    {
        if (col == null)
        {
            return null;
        }
        String colName = col.getName();
        if (colName != null && colName.indexOf(":") > 0)
        {
            return colName.substring(colName.indexOf(":") + 1);
        }
        return colName;
    }

    /**
     * Convenience method that extracts the version for a class of the specified type from the passed Result.
     * @param cmd Metadata for the class
     * @param result The result
     * @param ec ExecutionContext
     * @param table The table
     * @param storeMgr StoreManager
     * @return The version
     */
    public static Object getVersionForObject(AbstractClassMetaData cmd, Result result, ExecutionContext ec, Table table, StoreManager storeMgr)
    {
        String tableName = table.getName();
        if (cmd.isVersioned())
        {
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getMemberName() != null)
            {
                // Version stored in a field
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getMemberName());
                Column col = table.getMemberColumnMappingForMember(verMmd).getColumn(0);
                String familyName = HBaseUtils.getFamilyNameForColumn(col);
                String qualifName = HBaseUtils.getQualifierNameForColumn(col);
                Object version = null;
                try
                {
                    byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
                    if (vermd.getStrategy() == VersionStrategy.VERSION_NUMBER)
                    {
                        if (verMmd.getType() == Integer.class || verMmd.getType() == int.class)
                        {
                            // Field is integer based so use that
                            version = Bytes.toInt(bytes);
                        }
                        else
                        {
                            // Assume using Long
                            version = Bytes.toLong(bytes);
                        }
                    }
                    else if (Date.class.isAssignableFrom(verMmd.getType()))
                    {
                        // Field is of type Date (hence persisted as String), but version needs to be Timestamp
                        String strValue = new String(bytes);
                        TypeConverter strConv = ec.getTypeManager().getTypeConverterForType(verMmd.getType(), String.class);
                        version = strConv.toMemberType(strValue);
                        version = new Timestamp(((Date)version).getTime());
                    }
                    else
                    {
                        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                        ObjectInputStream ois = new ObjectInputStream(bis);
                        version = ois.readObject();
                        ois.close();
                        bis.close();
                    }
                }
                catch (Exception e)
                {
                    throw new NucleusException(e.getMessage(), e);
                }
                return version;
            }

            return getSurrogateVersionForObject(cmd, result, tableName, storeMgr);
        }
        return null;
    }

    /**
     * Convenience method that extracts the surrogate version for a class of the specified type from
     * the passed Result.
     * @param cmd Metadata for the class
     * @param result The result
     * @param tableName Name of the table
     * @param storeMgr Store Manager
     * @return The surrogate version
     */
    public static Object getSurrogateVersionForObject(AbstractClassMetaData cmd, Result result, String tableName, StoreManager storeMgr)
    {
        Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();
        VersionMetaData vermd = cmd.getVersionMetaDataForClass();

        String familyName = HBaseUtils.getFamilyNameForColumn(table.getSurrogateColumn(SurrogateColumnType.VERSION));
        String qualifName = HBaseUtils.getQualifierNameForColumn(table.getSurrogateColumn(SurrogateColumnType.VERSION));
        Object version = null;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
            if (vermd.getStrategy() == VersionStrategy.VERSION_NUMBER)
            {
                version = Bytes.toLong(bytes);
            }
            else
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                version = ois.readObject();
                ois.close();
                bis.close();
            }
        }
        catch (Exception e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return version;
    }

    public static Put getPutForObject(DNStateManager sm, Table schemaTable) throws IOException
    {
        byte[] rowKey = (byte[]) sm.getAssociatedValue("HBASE_ROW_KEY");
        if (rowKey == null)
        {
            AbstractClassMetaData cmd = sm.getClassMetaData();
            final Object[] pkValues = findKeyObjects(sm, cmd, schemaTable);
            ExecutionContext ec = sm.getExecutionContext();
            if (ec.getStatistics() != null)
            {
                // Add to statistics
                ec.getStatistics().incrementNumReads();
            }
            rowKey = getRowKeyForPkValue(pkValues, ec.getNucleusContext());
        }
        return new Put(rowKey);
    }

    public static Delete getDeleteForObject(DNStateManager sm, Table schemaTable) throws IOException
    {
        byte[] rowKey = (byte[]) sm.getAssociatedValue("HBASE_ROW_KEY");
        if (rowKey == null)
        {
            AbstractClassMetaData cmd = sm.getClassMetaData();
            final Object[] pkValues = findKeyObjects(sm, cmd, schemaTable);
            ExecutionContext ec = sm.getExecutionContext();
            if (ec.getStatistics() != null)
            {
                // Add to statistics
                ec.getStatistics().incrementNumReads();
            }
            rowKey = getRowKeyForPkValue(pkValues, ec.getNucleusContext());
        }
        return new Delete(rowKey);
    }

    public static Get getGetForObject(DNStateManager sm, Table schemaTable) throws IOException
    {
        byte[] rowKey = (byte[]) sm.getAssociatedValue("HBASE_ROW_KEY");
        if (rowKey == null)
        {
            AbstractClassMetaData cmd = sm.getClassMetaData();
            final Object[] pkValues = findKeyObjects(sm, cmd, schemaTable);
            ExecutionContext ec = sm.getExecutionContext();
            if (ec.getStatistics() != null)
            {
                // Add to statistics
                ec.getStatistics().incrementNumReads();
            }
            rowKey = getRowKeyForPkValue(pkValues, ec.getNucleusContext());
        }
        return new Get(rowKey);
    }

    public static Result getResultForObject(DNStateManager sm, org.apache.hadoop.hbase.client.Table table, Table schemaTable) throws IOException
    {
        Get get = getGetForObject(sm, schemaTable);
        return table.get(get);
    }

    public static boolean objectExistsInTable(DNStateManager sm, org.apache.hadoop.hbase.client.Table table, Table schemaTable) throws IOException
    {
        Get get = getGetForObject(sm, schemaTable);
        return table.exists(get);
    }

    private static Object[] findKeyObjects(final DNStateManager sm, final AbstractClassMetaData cmd, Table schemaTable)
    {
        if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            return new Object[]{IdentityUtils.getTargetKeyForDatastoreIdentity(sm.getInternalObjectId())};
        }
        else if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            final int[] pkFieldNums = cmd.getPKMemberPositions();
            List pkVals = new ArrayList();
            ExecutionContext ec = sm.getExecutionContext();
            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            for (int i = 0; i < pkFieldNums.length; i++)
            {
                AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[i]);
                RelationType relType = pkMmd.getRelationType(clr);
                Object fieldVal = sm.provideField(pkFieldNums[i]);
                if (relType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(sm.getStoreManager().getMetaDataManager(), clr, pkMmd, relType, null))
                {
                    // Embedded : allow 1 level of embedded field for PK
                    DNStateManager embSM = sm.getExecutionContext().findStateManager(fieldVal);
                    AbstractClassMetaData embCmd = embSM.getClassMetaData();
                    int[] memberPositions = embCmd.getAllMemberPositions();
                    for (int j=0;j<memberPositions.length;j++)
                    {
                        AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(memberPositions[j]);
                        if (embMmd.getPersistenceModifier() != FieldPersistenceModifier.PERSISTENT)
                        {
                            // Don't need column if not persistent
                            continue;
                        }

                        Object embFieldVal = embSM.provideField(memberPositions[j]);
                        pkVals.add(embFieldVal); // TODO Cater for field mapped to multiple columns
                    }
                }
                else
                {
                    MemberColumnMapping mapping = schemaTable.getMemberColumnMappingForMember(pkMmd);
                    if (RelationType.isRelationSingleValued(relType))
                    {
                        pkVals.add(IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter().getIdForObject(fieldVal)));
                    }
                    else
                    {
                        if (mapping.getTypeConverter() != null)
                        {
                            // Lookup using converted value
                            pkVals.add(mapping.getTypeConverter().toDatastoreType(fieldVal));
                        }
                        else
                        {
                            pkVals.add(fieldVal);
                        }
                    }
                }
            }
            return pkVals.toArray();
        }
        else
        {
            final int[] fieldNumbers = cmd.getAllMemberPositions();
            final Object[] keyObjects = new Object[fieldNumbers.length];
            for (int i = 0; i < fieldNumbers.length; i++)
            {
                keyObjects[i] = sm.provideField(fieldNumbers[i]);
            }
            return keyObjects;
        }
    }

    /**
     * Convenience method to generate the row key given the provided PK value(s).
     * @param pkValues Values of the PK field(s)
     * @param nucCtx NucleusContext
     * @return The row key
     * @throws IOException If an error occurs generating the row key
     */
    static byte[] getRowKeyForPkValue(Object[] pkValues, NucleusContext nucCtx) throws IOException
    {
        boolean useSerialisation = nucCtx.getConfiguration().getBooleanProperty(HBaseStoreManager.PROPERTY_HBASE_SERIALISED_PK);
        if (pkValues.length == 1 && !useSerialisation)
        {
            Object pkValue = pkValues[0];
            if (pkValue instanceof String)
            {
                return Bytes.toBytes((String) pkValue);
            }
            else if (pkValue instanceof Long)
            {
                return Bytes.toBytes(((Long) pkValue).longValue());
            }
            else if (pkValue instanceof Integer)
            {
                return Bytes.toBytes(((Integer) pkValue).intValue());
            }
            else if (pkValue instanceof Short)
            {
                return Bytes.toBytes(((Short) pkValue).shortValue());
            }
            // TODO Support other types?
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try
        {
            if (useSerialisation)
            {
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                try
                {
                    // Legacy data, up to and including DN 3.0.1
                    for (Object pkValue : pkValues)
                    {
                        oos.writeObject(pkValue);
                    }
                }
                finally
                {
                    oos.close();
                }
            }
            else
            {
                for (Object pkValue : pkValues)
                {
                    if (pkValue instanceof String)
                    {
                        bos.write(Bytes.toBytes((String) pkValue));
                    }
                    else if (pkValue instanceof Long)
                    {
                        bos.write(Bytes.toBytes(((Long) pkValue).longValue()));
                    }
                    else if (pkValue instanceof Integer)
                    {
                        bos.write(Bytes.toBytes(((Integer) pkValue).intValue()));
                    }
                    else if (pkValue instanceof Short)
                    {
                        bos.write(Bytes.toBytes(((Short) pkValue).shortValue()));
                    }
                    else
                    {
                        // Object serialisation approach. Can't keep that open from the very beginning, it messes up the byte stream.
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        try
                        {
                            oos.writeObject(pkValue);
                        }
                        finally
                        {
                            oos.close();
                        }
                    }
                }
            }
            return bos.toByteArray();
        }
        finally
        {
            bos.close();
        }
    }
}