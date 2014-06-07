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
2011 Andy Jefferson - clean up of NPE code
2011 Andy Jefferson - rewritten to support relationships
    ...
***********************************************************************/
package org.datanucleus.store.hbase.fieldmanager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFetchFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.hbase.HBaseUtils;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.store.types.converters.TypeConverterHelper;

/**
 * FieldManager to use for retrieving values from HBase to put into a persistable object.
 */
public class FetchFieldManager extends AbstractFetchFieldManager
{
    Table table;

    Result result;

    public FetchFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, Result result, Table table)
    {
        super(ec, cmd);
        this.result = result;
        this.table = table;
    }

    public FetchFieldManager(ObjectProvider op, Result result, Table table)
    {
        super(op);
        this.result = result;
        this.table = table;
    }

    protected AbstractMemberMetaData getMemberMetaData(int fieldNumber)
    {
        return cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        return table.getMemberColumnMappingForMember(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
    }

    public boolean fetchBooleanField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        String qualifName = HBaseUtils.getQualifierNameForColumn(col);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        return fetchBooleanInternal(bytes, mmd.isSerialized(), null);
    }

    public byte fetchByteField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        String qualifName = HBaseUtils.getQualifierNameForColumn(col);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        return fetchByteInternal(bytes, mmd.isSerialized(), null);
    }

    public char fetchCharField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        String qualifName = HBaseUtils.getQualifierNameForColumn(col);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        return fetchCharInternal(bytes, mmd.isSerialized(), null);
    }

    public double fetchDoubleField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        String qualifName = HBaseUtils.getQualifierNameForColumn(col);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        return fetchDoubleInternal(bytes, mmd.isSerialized(), null);
    }

    public float fetchFloatField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        String qualifName = HBaseUtils.getQualifierNameForColumn(col);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        return fetchFloatInternal(bytes, mmd.isSerialized(), null);
    }

    public int fetchIntField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        String qualifName = HBaseUtils.getQualifierNameForColumn(col);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        return fetchIntInternal(bytes, mmd.isSerialized(), null);
    }

    public long fetchLongField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        String qualifName = HBaseUtils.getQualifierNameForColumn(col);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        return fetchLongInternal(bytes, mmd.isSerialized(), null);
    }

    public short fetchShortField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        String qualifName = HBaseUtils.getQualifierNameForColumn(col);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        return fetchShortInternal(bytes, mmd.isSerialized(), null);
    }

    public String fetchStringField(int fieldNumber)
    {
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        String qualifName = HBaseUtils.getQualifierNameForColumn(col);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        if (bytes == null)
        {
            // TODO Get hold of default from column
            return null;
        }

        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        if (mmd.isSerialized())
        {
            // Early version of this plugin serialised values
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                String value = (String) ois.readObject();
                ois.close();
                bis.close();
                return value;
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
            catch (ClassNotFoundException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            return new String(bytes);
        }
    }

    public Object fetchObjectField(int fieldNumber)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        RelationType relationType = mmd.getRelationType(clr);
        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
        {
            // Embedded field
            if (RelationType.isRelationSingleValued(relationType))
            {
                // Persistable object embedded into table of this object
                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
                embMmds.add(mmd);

                Class embcls = mmd.getType();
                AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(embcls, clr);

                // Check for null value (currently need all columns to return null)
                // TODO Cater for null (use embmd.getNullIndicatorColumn/Value)
                boolean isNull = true;
                int[] embAllPosns = embCmd.getAllMemberPositions();
                for (int i=0;i<embAllPosns.length;i++)
                {
                    AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(i);
                    List<AbstractMemberMetaData> subEmbMmds = new ArrayList<AbstractMemberMetaData>(embMmds);
                    subEmbMmds.add(embMmd);
                    Column col = table.getMemberColumnMappingForEmbeddedMember(subEmbMmds).getColumn(0);
                    String familyName = HBaseUtils.getFamilyNameForColumn(col);
                    String columnName = HBaseUtils.getQualifierNameForColumn(col);
                    if (result.getValue(familyName.getBytes(), columnName.getBytes()) != null)
                    {
                        isNull = false;
                        break;
                    }
                }
                if (isNull)
                {
                    return null;
                }

                ObjectProvider embSM = ec.getNucleusContext().getObjectProviderFactory().newForEmbedded(ec, embCmd, op, fieldNumber);
                FieldManager ffm = new FetchEmbeddedFieldManager(embSM, result, embMmds, table);
                embSM.replaceFields(embCmd.getAllMemberPositions(), ffm);
                return embSM.getObject();
            }
            throw new NucleusUserException("Field " + mmd.getFullFieldName() + " marked as embedded not supported for this type");
        }

        return fetchNonEmbeddedObjectField(mmd, relationType, clr);
    }

    protected Object fetchNonEmbeddedObjectField(AbstractMemberMetaData mmd, RelationType relationType, ClassLoaderResolver clr)
    {
        int fieldNumber = mmd.getAbsoluteFieldNumber();
        MemberColumnMapping mapping = getColumnMapping(fieldNumber);

        Column col = mapping.getColumn(0); // TODO Support multicolumn converters
        String familyName = HBaseUtils.getFamilyNameForColumn(col);
        String qualifName = HBaseUtils.getQualifierNameForColumn(col);
        Object value = readObjectField(familyName, qualifName, result, fieldNumber, mmd);
        if (value == null)
        {
            return null;
        }

        if (RelationType.isRelationSingleValued(relationType))
        {
            if (mmd.isSerialized())
            {
                return value;
            }
            else
            {
                // The stored value was the identity
                return ec.findObject(value, true, true, null);
            }
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            if (mmd.hasCollection())
            {
                if (mmd.isSerialized())
                {
                    return value;
                }

                Collection<Object> coll;
                try
                {
                    Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                    coll = (Collection<Object>) instanceType.newInstance();
                }
                catch (Exception e)
                {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }

                Collection collIds = (Collection)value;
                Iterator idIter = collIds.iterator();
                while (idIter.hasNext())
                {
                    Object elementId = idIter.next();
                    coll.add(ec.findObject(elementId, true, true, null));
                }
                if (op != null)
                {
                    return op.wrapSCOField(fieldNumber, coll, false, false, true);
                }
                return coll;
            }
            else if (mmd.hasMap())
            {
                if (mmd.isSerialized())
                {
                    return value;
                }

                Map map;
                try
                {
                    Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), false);
                    map = (Map) instanceType.newInstance();
                }
                catch (Exception e)
                {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }

                Map mapIds = (Map)value;
                Iterator<Map.Entry> mapIdIter = mapIds.entrySet().iterator();
                while (mapIdIter.hasNext())
                {
                    Map.Entry entry = mapIdIter.next();
                    Object mapKey = entry.getKey();
                    Object mapValue = entry.getValue();
                    if (mmd.getMap().getKeyClassMetaData(clr, ec.getMetaDataManager()) != null)
                    {
                        // Map key must be an "id"
                        mapKey = ec.findObject(mapKey, true, true, null);
                    }
                    if (mmd.getMap().getValueClassMetaData(clr, ec.getMetaDataManager()) != null)
                    {
                        // Map value must be an "id"
                        mapValue = ec.findObject(mapValue, true, true, null);
                    }
                    map.put(mapKey, mapValue);
                }
                if (op != null)
                {
                    return op.wrapSCOField(fieldNumber, map, false, false, true);
                }
                return map;
            }
            else if (mmd.hasArray())
            {
                if (mmd.isSerialized())
                {
                    return value;
                }

                Collection arrIds = (Collection)value;
                Object array = Array.newInstance(mmd.getType().getComponentType(), arrIds.size());
                Iterator idIter = arrIds.iterator();
                int i=0;
                while (idIter.hasNext())
                {
                    Object elementId = idIter.next();
                    Array.set(array, i, ec.findObject(elementId, true, true, null));
                }
                return array;
            }
            throw new NucleusUserException("No container that isnt collection/map/array");
        }
        else
        {
            Object returnValue = value;
            if (!mmd.isSerialized())
            {
                if (mmd.getTypeConverterName() != null)
                {
                    // User-defined type converter
                    byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
                    TypeConverter conv = ec.getNucleusContext().getTypeManager().getTypeConverterForName(mmd.getTypeConverterName());
                    Class datastoreType = TypeConverterHelper.getDatastoreTypeForTypeConverter(conv, mmd.getType());
                    if (datastoreType == String.class)
                    {
                        returnValue = conv.toMemberType((String)value);
                    }
                    else if (datastoreType == Long.class)
                    {
                        returnValue = conv.toMemberType(Bytes.toLong(bytes));
                    }
                    else if (datastoreType == Integer.class)
                    {
                        returnValue = conv.toMemberType(Bytes.toInt(bytes));
                    }
                    else if (datastoreType == Double.class)
                    {
                        returnValue = conv.toMemberType(Bytes.toDouble(bytes));
                    }
                    else if (datastoreType == Boolean.class)
                    {
                        returnValue = conv.toMemberType(Bytes.toBoolean(bytes));
                    }
                }
                else if (Boolean.class.isAssignableFrom(mmd.getType()) ||
                        Byte.class.isAssignableFrom(mmd.getType()) ||
                        Integer.class.isAssignableFrom(mmd.getType()) ||
                        Double.class.isAssignableFrom(mmd.getType()) ||
                        Float.class.isAssignableFrom(mmd.getType()) ||
                        Long.class.isAssignableFrom(mmd.getType()) ||
                        Character.class.isAssignableFrom(mmd.getType()) ||
                        Short.class.isAssignableFrom(mmd.getType()))
                {
                    return value;
                }
                else if (Enum.class.isAssignableFrom(mmd.getType()))
                {
                    ColumnMetaData colmd = null;
                    if (mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
                    {
                        colmd = mmd.getColumnMetaData()[0];
                    }
                    if (MetaDataUtils.persistColumnAsNumeric(colmd))
                    {
                        return mmd.getType().getEnumConstants()[((Number)value).intValue()];
                    }
                    else
                    {
                        return Enum.valueOf(mmd.getType(), (String)value);
                    }
                }
                else
                {
                    // Fallback to built-in type converter
                    // TODO Make use of default TypeConverter for a type before falling back to String/Long
                    TypeConverter strConv = ec.getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
                    if (strConv != null)
                    {
                        // Persisted as a String, so convert back
                        String strValue = (String)value;
                        returnValue = strConv.toMemberType(strValue);
                    }
                }
            }
            if (op != null)
            {
                return op.wrapSCOField(fieldNumber, returnValue, false, false, true);
            }
            return returnValue;
        }
    }

    protected Object readObjectField(String familyName, String columnName, Result result, int fieldNumber, AbstractMemberMetaData mmd)
    {
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
        if (bytes == null)
        {
            return null;
        }

        // TODO Get default value for column from Table/Column structure
        if (mmd.getType() == Boolean.class)
        {
            return fetchBooleanInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
        }
        else if (mmd.getType() == Byte.class)
        {
            return fetchByteInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
        }
        else if (mmd.getType() == Character.class)
        {
            return fetchCharInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
        }
        else if (mmd.getType() == Double.class)
        {
            return fetchDoubleInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
        }
        else if (mmd.getType() == Float.class)
        {
            return fetchFloatInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
        }
        else if (mmd.getType() == Integer.class)
        {
            return fetchIntInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
        }
        else if (mmd.getType() == Long.class)
        {
            return fetchLongInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
        }
        else if (mmd.getType() == Short.class)
        {
            return fetchShortInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
        }
        else if (Enum.class.isAssignableFrom(mmd.getType()))
        {
            ColumnMetaData colmd = null;
            if (mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
            {
                colmd = mmd.getColumnMetaData()[0];
            }
            if (MetaDataUtils.persistColumnAsNumeric(colmd))
            {
                return fetchIntInternal(bytes, mmd.isSerialized(), HBaseUtils.getDefaultValueForMember(mmd));
            }
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = null;
        try
        {
            ois = new ObjectInputStream(bis);
            return ois.readObject();
        }
        catch (IOException e)
        {
            // Failure in deserialisation, so must be persisted as String
            // Return as a String TODO Allow persist using ObjectLongConverter as non-serialised
            return new String(bytes);
        }
        catch (ClassNotFoundException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        finally
        {
            try
            {
                if (ois != null)
                {
                    ois.close();
                }
                bis.close();
            }
            catch (IOException ioe)
            {
                throw new NucleusException(ioe.getMessage(), ioe);
            }
        }
    }

    private boolean fetchBooleanInternal(byte[] bytes, boolean serialised, String defaultValue)
    {
        boolean value;
        if (bytes == null)
        {
            // Handle missing field
            if (defaultValue != null)
            {
                return Boolean.valueOf(defaultValue).booleanValue();
            }
            return false;
        }

        if (serialised)
        {
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = ois.readBoolean();
                ois.close();
                bis.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            value = Bytes.toBoolean(bytes);
        }
        return value;
    }

    private byte fetchByteInternal(byte[] bytes, boolean serialised, String defaultValue)
    {
        if (bytes == null)
        {
            // Handle missing field
            if (defaultValue != null)
            {
                return defaultValue.getBytes()[0];
            }
            return 0;
        }

        return bytes[0];
    }

    private char fetchCharInternal(byte[] bytes, boolean serialised, String defaultValue)
    {
        char value;
        if (bytes == null)
        {
            // Handle missing field
            if (defaultValue != null && defaultValue.length() > 0)
            {
                return defaultValue.charAt(0);
            }
            return 0;
        }

        if (serialised)
        {
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = ois.readChar();
                ois.close();
                bis.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            String strValue = new String(bytes);
            value = strValue.charAt(0);
        }
        return value;
    }

    private double fetchDoubleInternal(byte[] bytes, boolean serialised, String defaultValue)
    {
        double value;
        if (bytes == null)
        {
            // Handle missing field
            if (defaultValue != null)
            {
                return Double.valueOf(defaultValue).doubleValue();
            }
            return 0;
        }

        if (serialised)
        {
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = ois.readDouble();
                ois.close();
                bis.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            value = Bytes.toDouble(bytes);
        }
        return value;
    }

    private float fetchFloatInternal(byte[] bytes, boolean serialised, String defaultValue)
    {
        float value;
        if (bytes == null)
        {
            // Handle missing field
            if (defaultValue != null)
            {
                return Float.valueOf(defaultValue).floatValue();
            }
            return 0;
        }

        if (serialised)
        {
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = ois.readFloat();
                ois.close();
                bis.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            value = Bytes.toFloat(bytes);
        }
        return value;
    }

    private int fetchIntInternal(byte[] bytes, boolean serialised, String defaultValue)
    {
        int value;
        if (bytes == null)
        {
            // Handle missing field
            if (defaultValue != null)
            {
                return Integer.valueOf(defaultValue).intValue();
            }
            return 0;
        }

        if (serialised)
        {
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = ois.readInt();
                ois.close();
                bis.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            value = Bytes.toInt(bytes);
        }
        return value;
    }

    private long fetchLongInternal(byte[] bytes, boolean serialised, String defaultValue)
    {
        long value;
        if (bytes == null)
        {
            // Handle missing field
            if (defaultValue != null)
            {
                return Long.valueOf(defaultValue).longValue();
            }
            return 0;
        }

        if (serialised)
        {
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = ois.readLong();
                ois.close();
                bis.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            value = Bytes.toLong(bytes);
        }
        return value;
    }

    private short fetchShortInternal(byte[] bytes, boolean serialised, String defaultValue)
    {
        short value;
        if (bytes == null)
        {
            // Handle missing field
            if (defaultValue != null)
            {
                return Short.valueOf(defaultValue).shortValue();
            }
            return 0;
        }

        if (serialised)
        {
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = ois.readShort();
                ois.close();
                bis.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            value = Bytes.toShort(bytes);
        }
        return value;
    }
}