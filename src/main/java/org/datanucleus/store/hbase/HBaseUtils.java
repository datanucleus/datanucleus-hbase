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
import java.util.Date;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.ExecutionContext;
import org.datanucleus.NucleusContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.schema.table.Column;
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
     * @param tableName Name of the table
     * @param storeMgr StoreManager
     * @return The version
     */
    public static Object getVersionForObject(AbstractClassMetaData cmd, Result result, ExecutionContext ec, Table table, StoreManager storeMgr)
    {
        String tableName = table.getName();
        if (cmd.isVersioned())
        {
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getFieldName() != null)
            {
                // Version stored in a field
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                Column col = table.getMemberColumnMappingForMember(verMmd).getColumn(0);
                String familyName = HBaseUtils.getFamilyNameForColumn(col);
                String qualifName = HBaseUtils.getQualifierNameForColumn(col);
                Object version = null;
                try
                {
                    byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
                    if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
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

        String familyName = HBaseUtils.getFamilyNameForColumn(table.getVersionColumn());
        String qualifName = HBaseUtils.getQualifierNameForColumn(table.getVersionColumn());
        Object version = null;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
            if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
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

    public static Put getPutForObject(ObjectProvider op) throws IOException
    {
        byte[] rowKey = (byte[]) op.getAssociatedValue("HBASE_ROW_KEY");
        if (rowKey == null)
        {
            AbstractClassMetaData cmd = op.getClassMetaData();
            final Object[] pkValues = findKeyObjects(op, cmd);
            ExecutionContext ec = op.getExecutionContext();
            if (ec.getStatistics() != null)
            {
                // Add to statistics
                ec.getStatistics().incrementNumReads();
            }
            rowKey = getRowKeyForPkValue(pkValues, ec.getNucleusContext());
        }
        return new Put(rowKey);
    }

    public static Delete getDeleteForObject(ObjectProvider op) throws IOException
    {
        byte[] rowKey = (byte[]) op.getAssociatedValue("HBASE_ROW_KEY");
        if (rowKey == null)
        {
            AbstractClassMetaData cmd = op.getClassMetaData();
            final Object[] pkValues = findKeyObjects(op, cmd);
            ExecutionContext ec = op.getExecutionContext();
            if (ec.getStatistics() != null)
            {
                // Add to statistics
                ec.getStatistics().incrementNumReads();
            }
            rowKey = getRowKeyForPkValue(pkValues, ec.getNucleusContext());
        }
        return new Delete(rowKey);
    }

    public static Get getGetForObject(ObjectProvider op) throws IOException
    {
        byte[] rowKey = (byte[]) op.getAssociatedValue("HBASE_ROW_KEY");
        if (rowKey == null)
        {
            AbstractClassMetaData cmd = op.getClassMetaData();
            final Object[] pkValues = findKeyObjects(op, cmd);
            ExecutionContext ec = op.getExecutionContext();
            if (ec.getStatistics() != null)
            {
                // Add to statistics
                ec.getStatistics().incrementNumReads();
            }
            rowKey = getRowKeyForPkValue(pkValues, ec.getNucleusContext());
        }
        return new Get(rowKey);
    }

    public static Result getResultForObject(ObjectProvider op, HTableInterface table) throws IOException
    {
        Get get = getGetForObject(op);
        return table.get(get);
    }

    public static boolean objectExistsInTable(ObjectProvider op, HTableInterface table) throws IOException
    {
        Get get = getGetForObject(op);
        return table.exists(get);
    }

    private static Object[] findKeyObjects(final ObjectProvider op, final AbstractClassMetaData cmd)
    {
        if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            return new Object[]{IdentityUtils.getTargetKeyForDatastoreIdentity(op.getInternalObjectId())};
        }
        else if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            final int[] fieldNumbers = op.getClassMetaData().getPKMemberPositions();
            final Object[] keyObjects = new Object[fieldNumbers.length];
            for (int i = 0; i < fieldNumbers.length; i++)
            {
                keyObjects[i] = op.provideField(fieldNumbers[i]);
            }
            return keyObjects;
        }
        else
        {
            final int[] fieldNumbers = op.getClassMetaData().getAllMemberPositions();
            final Object[] keyObjects = new Object[fieldNumbers.length];
            for (int i = 0; i < fieldNumbers.length; i++)
            {
                keyObjects[i] = op.provideField(fieldNumbers[i]);
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
        boolean useSerialisation = nucCtx.getConfiguration().getBooleanProperty("datanucleus.hbase.serialisedPK");
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
                        // Object serialisation approach. Can't keep that open from the very
                        // beginning, it messes up the byte stream.
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