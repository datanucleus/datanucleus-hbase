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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractStoreFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.hbase.HBaseUtils;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.store.types.converters.TypeConverterHelper;

public class StoreFieldManager extends AbstractStoreFieldManager
{
    Put put;
    Delete delete;
    String tableName;

    public StoreFieldManager(ObjectProvider op, Put put, Delete delete, boolean insert, String tableName)
    {
        super(op, insert);

        this.put = put;
        this.delete = delete;
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

    public void storeBooleanField(int fieldNumber, boolean value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        storeBooleanInternal(mmd, familyName, qualifName, value);
    }

    public void storeByteField(int fieldNumber, byte value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        storeByteInternal(mmd, familyName, qualifName, value);
    }

    public void storeCharField(int fieldNumber, char value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        storeCharInternal(mmd, familyName, qualifName, value);
    }

    public void storeDoubleField(int fieldNumber, double value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        storeDoubleInternal(mmd, familyName, qualifName, value);
    }

    public void storeFloatField(int fieldNumber, float value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        storeFloatInternal(mmd, familyName, qualifName, value);
    }

    public void storeIntField(int fieldNumber, int value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        storeIntInternal(mmd, familyName, qualifName, value);
    }

    public void storeLongField(int fieldNumber, long value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        storeLongInternal(mmd, familyName, qualifName, value);
    }

    public void storeShortField(int fieldNumber, short value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        storeShortInternal(mmd, familyName, qualifName, value);
    }

    public void storeStringField(int fieldNumber, String value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        if (value == null)
        {
            delete.deleteColumn(familyName.getBytes(), qualifName.getBytes());
        }
        else
        {
            AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
            if (mmd.isSerialized())
            {
                writeObjectField(familyName, qualifName, value);
            }
            else
            {
                put.add(familyName.getBytes(), qualifName.getBytes(), value.getBytes());
            }
        }
    }

    public void storeObjectField(int fieldNumber, Object value)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        if (!isStorable(mmd))
        {
            return;
        }

        RelationType relationType = mmd.getRelationType(clr);
        if (RelationType.isRelationSingleValued(relationType) && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
        {
            // Embedded PC object
            Class embcls = mmd.getType();
            AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(embcls, clr);
            if (embcmd != null) 
            {
                if (value == null)
                {
                    deleteColumnsForEmbeddedMember(mmd, clr, ec);
                    return;
                }

                ObjectProvider embSM = ec.findObjectProviderForEmbedded(value, op, mmd);
                FieldManager ffm = new StoreEmbeddedFieldManager(embSM, put, delete, mmd, tableName, insert);
                embSM.provideFields(embcmd.getAllMemberPositions(), ffm);
                return;
            }
            else
            {
                throw new NucleusUserException("Field " + mmd.getFullFieldName() +
                    " specified as embedded but metadata not found for the class of type " + mmd.getTypeName());
            }
        }

        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        if (value == null)
        {
            // TODO What about delete-orphans?
            delete.deleteColumn(familyName.getBytes(), qualifName.getBytes());
        }
        else
        {
            if (RelationType.isRelationSingleValued(relationType))
            {
                // PC object, so make sure it is persisted
                Object valuePC = ec.persistObjectInternal(value, op, fieldNumber, -1);
                if (mmd.isSerialized())
                {
                    // Persist as serialised into the column of this object
                    writeObjectField(familyName, qualifName, value);
                }
                else
                {
                    // Persist identity in the column of this object
                    Object valueId = ec.getApiAdapter().getIdForObject(valuePC);
                    writeObjectField(familyName, qualifName, valueId);
                }
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                // Collection/Map/Array
                if (mmd.hasCollection())
                {
                    Collection collIds = new ArrayList();
                    Collection coll = (Collection)value;
                    Iterator collIter = coll.iterator();
                    while (collIter.hasNext())
                    {
                        Object element = collIter.next();
                        Object elementPC = ec.persistObjectInternal(element, op, fieldNumber, -1);
                        Object elementID = ec.getApiAdapter().getIdForObject(elementPC);
                        collIds.add(elementID);
                    }

                    if (mmd.isSerialized())
                    {
                        // Persist as serialised into the column of this object
                        writeObjectField(familyName, qualifName, value);
                    }
                    else
                    {
                        // Persist list<ids> into the column of this object
                        writeObjectField(familyName, qualifName, collIds);
                    }
                    op.wrapSCOField(fieldNumber, value, false, false, true);
                }
                else if (mmd.hasMap())
                {
                    Map mapIds = new HashMap();
                    Map map = (Map)value;
                    Iterator<Map.Entry> mapIter = map.entrySet().iterator();
                    while (mapIter.hasNext())
                    {
                        Map.Entry entry = mapIter.next();
                        Object mapKey = entry.getKey();
                        Object mapValue = entry.getValue();
                        if (ec.getApiAdapter().isPersistable(mapKey))
                        {
                            Object pKey = ec.persistObjectInternal(mapKey, op, fieldNumber, -1);
                            mapKey = ec.getApiAdapter().getIdForObject(pKey);
                        }
                        if (ec.getApiAdapter().isPersistable(mapValue))
                        {
                            Object pVal = ec.persistObjectInternal(mapValue, op, fieldNumber, -1);
                            mapValue = ec.getApiAdapter().getIdForObject(pVal);
                        }
                        mapIds.put(mapKey, mapValue);
                    }

                    if (mmd.isSerialized())
                    {
                        // Persist as serialised into the column of this object
                        writeObjectField(familyName, qualifName, value);
                    }
                    else
                    {
                        // Persist map<keyids,valids> into the column of this object
                        writeObjectField(familyName, qualifName, mapIds);
                    }
                    op.wrapSCOField(fieldNumber, value, false, false, true);
                }
                else if (mmd.hasArray())
                {
                    Collection arrIds = new ArrayList();
                    for (int i=0;i<Array.getLength(value);i++)
                    {
                        Object element = Array.get(value, i);
                        Object elementPC = ec.persistObjectInternal(element, op, fieldNumber, -1);
                        Object elementID = ec.getApiAdapter().getIdForObject(elementPC);
                        arrIds.add(elementID);
                    }

                    if (mmd.isSerialized())
                    {
                        // Persist as serialised into the column of this object
                        writeObjectField(familyName, qualifName, value);
                    }
                    else
                    {
                        // Persist list of array element ids into the column of this object
                        writeObjectField(familyName, qualifName, arrIds);
                    }
                }
            }
            else
            {
                if (!mmd.isSerialized())
                {
                    if (mmd.getTypeConverterName() != null)
                    {
                        // User-defined converter
                        TypeManager typeMgr = ec.getNucleusContext().getTypeManager();
                        TypeConverter conv = typeMgr.getTypeConverterForName(mmd.getTypeConverterName());
                        Class datastoreType = TypeConverterHelper.getDatastoreTypeForTypeConverter(conv, mmd.getType());
                        if (datastoreType == String.class)
                        {
                            String strValue = (String)conv.toDatastoreType(value);
                            put.add(familyName.getBytes(), qualifName.getBytes(), strValue.getBytes());
                            return;
                        }
                        else if (datastoreType == Boolean.class)
                        {
                            storeBooleanInternal(mmd, familyName, qualifName, (Boolean)value);
                            return;
                        }
                        else if (datastoreType == Double.class)
                        {
                            storeDoubleInternal(mmd, familyName, qualifName, (Double)value);
                            return;
                        }
                        else if (datastoreType == Integer.class)
                        {
                            storeIntInternal(mmd, familyName, qualifName, (Integer)value);
                            return;
                        }
                        else if (datastoreType == Long.class)
                        {
                            storeLongInternal(mmd, familyName, qualifName, (Long)value);
                            return;
                        }
                    }
                    if (Boolean.class.isAssignableFrom(value.getClass()))
                    {
                        storeBooleanInternal(mmd, familyName, qualifName, (Boolean)value);
                        return;
                    }
                    else if (Byte.class.isAssignableFrom(value.getClass()))
                    {
                        storeByteInternal(mmd, familyName, qualifName, (Byte)value);
                        return;
                    }
                    else if (Character.class.isAssignableFrom(value.getClass()))
                    {
                        storeCharInternal(mmd, familyName, qualifName, (Character)value);
                        return;
                    }
                    else if (Double.class.isAssignableFrom(value.getClass()))
                    {
                        storeDoubleInternal(mmd, familyName, qualifName, (Double)value);
                        return;
                    }
                    else if (Float.class.isAssignableFrom(value.getClass()))
                    {
                        storeFloatInternal(mmd, familyName, qualifName, (Float)value);
                        return;
                    }
                    else if (Integer.class.isAssignableFrom(value.getClass()))
                    {
                        storeIntInternal(mmd, familyName, qualifName, (Integer)value);
                        return;
                    }
                    else if (Long.class.isAssignableFrom(value.getClass()))
                    {
                        storeLongInternal(mmd, familyName, qualifName, (Long)value);
                        return;
                    }
                    else if (Short.class.isAssignableFrom(value.getClass()))
                    {
                        storeShortInternal(mmd, familyName, qualifName, (Short)value);
                        return;
                    }
                    else if (Enum.class.isAssignableFrom(value.getClass()))
                    {
                        ColumnMetaData colmd = null;
                        if (mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
                        {
                            colmd = mmd.getColumnMetaData()[0];
                        }
                        if (MetaDataUtils.persistColumnAsNumeric(colmd))
                        {
                            put.add(familyName.getBytes(), qualifName.getBytes(), Bytes.toBytes(((Enum)value).ordinal()));
                        }
                        else
                        {
                            put.add(familyName.getBytes(), qualifName.getBytes(), ((Enum)value).name().getBytes());
                        }
                        return;
                    }

                    // Fallback to built-in String converters
                    // TODO Make use of default TypeConverter for a type before falling back to String/Long
                    TypeConverter strConv = ec.getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
                    if (strConv != null)
                    {
                        // Persist as a String
                        String strValue = (String)strConv.toDatastoreType(value);
                        put.add(familyName.getBytes(), qualifName.getBytes(), strValue.getBytes());
                        return;
                    }
                }

                // Persist serialised
                writeObjectField(familyName, qualifName, value);
                op.wrapSCOField(fieldNumber, value, false, false, true);
            }
        }
    }

    protected void writeObjectField(String familyName, String columnName, Object value)
    {
        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(value);
            put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
            oos.close();
            bos.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    protected void deleteColumnsForEmbeddedMember(AbstractMemberMetaData mmd, ClassLoaderResolver clr,
            ExecutionContext ec)
    {
        Class embcls = mmd.getType();
        AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(embcls, clr);
        if (embcmd != null)
        {
            EmbeddedMetaData embmd = mmd.getEmbeddedMetaData();
            AbstractMemberMetaData[] embmmds = embmd.getMemberMetaData();
            for (int i=0;i<embmmds.length;i++)
            {
                RelationType relationType = embmmds[i].getRelationType(clr);
                if (RelationType.isRelationSingleValued(relationType) && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, embmmds[i], relationType, mmd))
                {
                    deleteColumnsForEmbeddedMember(embmmds[i], clr, ec);
                }
                else
                {
                    String familyName = HBaseUtils.getFamilyName(mmd, i, tableName);
                    String columnName = HBaseUtils.getQualifierName(mmd, i);
                    delete.deleteColumn(familyName.getBytes(), columnName.getBytes());
                }
            }
            return;
        }
    }

    private void storeBooleanInternal(AbstractMemberMetaData mmd, String familyName, String columnName, boolean value)
    {
        if (mmd.isSerialized())
        {
            try
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeBoolean(value);
                oos.flush();
                put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                oos.close();
                bos.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            put.add(familyName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
        }
    }

    private void storeByteInternal(AbstractMemberMetaData mmd, String familyName, String columnName, byte value)
    {
        put.add(familyName.getBytes(), columnName.getBytes(), new byte[]{value});
    }

    private void storeCharInternal(AbstractMemberMetaData mmd, String familyName, String columnName, char value)
    {
        if (mmd.isSerialized())
        {
            try
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeChar(value);
                oos.flush();
                put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                oos.close();
                bos.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            put.add(familyName.getBytes(), columnName.getBytes(), ("" + value).getBytes());
        }
    }

    private void storeDoubleInternal(AbstractMemberMetaData mmd, String familyName, String columnName, double value)
    {
        if (mmd.isSerialized())
        {
            try
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeDouble(value);
                oos.flush();
                put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                oos.close();
                bos.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            put.add(familyName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
        }
    }

    private void storeFloatInternal(AbstractMemberMetaData mmd, String familyName, String columnName, float value)
    {
        if (mmd.isSerialized())
        {
            try
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeFloat(value);
                oos.flush();
                put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                oos.close();
                bos.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            put.add(familyName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
        }
    }

    private void storeIntInternal(AbstractMemberMetaData mmd, String familyName, String columnName, int value)
    {
        if (mmd.isSerialized())
        {
            try
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeInt(value);
                oos.flush();
                put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                oos.close();
                bos.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            put.add(familyName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
        }
    }

    private void storeLongInternal(AbstractMemberMetaData mmd, String familyName, String columnName, long value)
    {
        if (mmd.isSerialized())
        {
            try
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeLong(value);
                oos.flush();
                put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                oos.close();
                bos.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            put.add(familyName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
        }
    }

    private void storeShortInternal(AbstractMemberMetaData mmd, String familyName, String columnName, short value)
    {
        if (mmd.isSerialized())
        {
            try
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeShort(value);
                oos.flush();
                put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                oos.close();
                bos.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            put.add(familyName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
        }
   }
}