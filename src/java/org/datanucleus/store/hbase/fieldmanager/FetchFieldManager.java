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
import java.util.Collection;
import java.util.Iterator;
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
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFetchFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.hbase.HBaseUtils;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.store.types.converters.TypeConverter;

public class FetchFieldManager extends AbstractFetchFieldManager
{
    Result result;
    String tableName;

    public FetchFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, Result result, String tableName)
    {
        super(ec, cmd);
        this.result = result;
        this.tableName = tableName;
    }

    public FetchFieldManager(ObjectProvider op, Result result, String tableName)
    {
        super(op);
        this.result = result;
        this.tableName = tableName;
    }

    protected String getFamilyName(int fieldNumber)
    {
        return HBaseUtils.getFamilyName(cmd, fieldNumber, tableName);
    }

    protected String getQualifierName(int fieldNumber)
    {
        return HBaseUtils.getQualifierName(cmd, fieldNumber);
    }

    protected AbstractMemberMetaData getMemberMetaData(int fieldNumber)
    {
        return cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
    }

    public boolean fetchBooleanField(int fieldNumber)
    {
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        return fetchBooleanInternal(mmd, bytes);
    }

    public byte fetchByteField(int fieldNumber)
    {
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        return fetchByteInternal(mmd, bytes);
    }

    public char fetchCharField(int fieldNumber)
    {
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        return fetchCharInternal(mmd, bytes);
    }

    public double fetchDoubleField(int fieldNumber)
    {
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        return fetchDoubleInternal(mmd, bytes);
    }

    public float fetchFloatField(int fieldNumber)
    {
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        return fetchFloatInternal(mmd, bytes);
    }

    public int fetchIntField(int fieldNumber)
    {
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        return fetchIntInternal(mmd, bytes);
    }

    public long fetchLongField(int fieldNumber)
    {
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        return fetchLongInternal(mmd, bytes);
    }

    public short fetchShortField(int fieldNumber)
    {
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        return fetchShortInternal(mmd, bytes);
    }

    public String fetchStringField(int fieldNumber)
    {
        String value;
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), qualifName.getBytes());
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        if (bytes == null)
        {
            // Handle missing field
            return HBaseUtils.getDefaultValueForMember(mmd);
        }

        if (mmd.isSerialized())
        {
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = (String) ois.readObject();
                ois.close();
                bis.close();
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
            value = new String(bytes);
        }
        return value;
    }

    public Object fetchObjectField(int fieldNumber)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        RelationType relationType = mmd.getRelationType(clr);
        if (mmd.isEmbedded() && RelationType.isRelationSingleValued(relationType))
        {
            // Persistable object embedded into table of this object
            Class embcls = mmd.getType();
            AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(embcls, clr);
            if (embcmd != null)
            {
                // Check for null value (currently need all columns to return null)
                // TODO Cater for null (use embmd.getNullIndicatorColumn/Value)
                EmbeddedMetaData embmd = mmd.getEmbeddedMetaData();
                AbstractMemberMetaData[] embmmds = embmd.getMemberMetaData();
                boolean isNull = true;
                for (int i=0;i<embmmds.length;i++)
                {
                    String familyName = HBaseUtils.getFamilyName(mmd, i, tableName);
                    String columnName = HBaseUtils.getQualifierName(mmd, i);
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

                ObjectProvider embSM = ec.newObjectProviderForEmbedded(embcmd, op, fieldNumber);
                FieldManager ffm = new FetchEmbeddedFieldManager(embSM, result, mmd, tableName);
                embSM.replaceFields(embcmd.getAllMemberPositions(), ffm);
                return embSM.getObject();
            }
            throw new NucleusUserException("Field " + mmd.getFullFieldName() + " marked as embedded but no such metadata");
        }

        String familyName = HBaseUtils.getFamilyName(cmd, fieldNumber, tableName);
        String qualifName = HBaseUtils.getQualifierName(cmd, fieldNumber);
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
                    Class datastoreType = TypeManager.getDatastoreTypeForTypeConverter(conv, mmd.getType());
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

    protected Object readObjectField(String familyName, String columnName, Result result, int fieldNumber,
            AbstractMemberMetaData mmd)
    {
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
        if (bytes == null)
        {
            return null;
        }

        if (mmd.getType() == Boolean.class)
        {
            return fetchBooleanInternal(mmd, bytes);
        }
        else if (mmd.getType() == Byte.class)
        {
            return fetchByteInternal(mmd, bytes);
        }
        else if (mmd.getType() == Character.class)
        {
            return fetchCharInternal(mmd, bytes);
        }
        else if (mmd.getType() == Double.class)
        {
            return fetchDoubleInternal(mmd, bytes);
        }
        else if (mmd.getType() == Float.class)
        {
            return fetchFloatInternal(mmd, bytes);
        }
        else if (mmd.getType() == Integer.class)
        {
            return fetchIntInternal(mmd, bytes);
        }
        else if (mmd.getType() == Long.class)
        {
            return fetchLongInternal(mmd, bytes);
        }
        else if (mmd.getType() == Short.class)
        {
            return fetchShortInternal(mmd, bytes);
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
                return fetchIntInternal(mmd, bytes);
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

    private boolean fetchBooleanInternal(AbstractMemberMetaData mmd, byte[] bytes)
    {
        boolean value;
        if (bytes == null)
        {
            // Handle missing field
            String dflt = HBaseUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return Boolean.valueOf(dflt).booleanValue();
            }
            return false;
        }

        if (mmd.isSerialized())
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

    private byte fetchByteInternal(AbstractMemberMetaData mmd, byte[] bytes)
    {
        if (bytes == null)
        {
            // Handle missing field
            String dflt = HBaseUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return dflt.getBytes()[0];
            }
            return 0;
        }

        return bytes[0];
    }

    private char fetchCharInternal(AbstractMemberMetaData mmd, byte[] bytes)
    {
        char value;
        if (bytes == null)
        {
            // Handle missing field
            String dflt = HBaseUtils.getDefaultValueForMember(mmd);
            if (dflt != null && dflt.length() > 0)
            {
                return dflt.charAt(0);
            }
            return 0;
        }

        if (mmd.isSerialized())
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

    private double fetchDoubleInternal(AbstractMemberMetaData mmd, byte[] bytes)
    {
        double value;
        if (bytes == null)
        {
            // Handle missing field
            String dflt = HBaseUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return Double.valueOf(dflt).doubleValue();
            }
            return 0;
        }

        if (mmd.isSerialized())
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

    private float fetchFloatInternal(AbstractMemberMetaData mmd, byte[] bytes)
    {
        float value;
        if (bytes == null)
        {
            // Handle missing field
            String dflt = HBaseUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return Float.valueOf(dflt).floatValue();
            }
            return 0;
        }

        if (mmd.isSerialized())
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

    private int fetchIntInternal(AbstractMemberMetaData mmd, byte[] bytes)
    {
        int value;
        if (bytes == null)
        {
            // Handle missing field
            String dflt = HBaseUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return Integer.valueOf(dflt).intValue();
            }
            return 0;
        }

        if (mmd.isSerialized())
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

    private long fetchLongInternal(AbstractMemberMetaData mmd, byte[] bytes)
    {
        long value;
        if (bytes == null)
        {
            // Handle missing field
            String dflt = HBaseUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return Long.valueOf(dflt).longValue();
            }
            return 0;
        }

        if (mmd.isSerialized())
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

    private short fetchShortInternal(AbstractMemberMetaData mmd, byte[] bytes)
    {
        short value;
        if (bytes == null)
        {
            // Handle missing field
            String dflt = HBaseUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return Short.valueOf(dflt).shortValue();
            }
            return 0;
        }

        if (mmd.isSerialized())
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