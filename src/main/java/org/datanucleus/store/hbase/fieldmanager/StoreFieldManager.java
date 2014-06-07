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
import java.util.List;
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
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractStoreFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.hbase.HBaseUtils;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.store.types.converters.TypeConverterHelper;
import org.datanucleus.util.ClassUtils;

/**
 * FieldManager to use for storing values into HBase from a managed persistable object.
 */
public class StoreFieldManager extends AbstractStoreFieldManager
{
    Table table;

    Put put;

    Delete delete;

    public StoreFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, Put put, Delete delete, boolean insert, Table table)
    {
        super(ec, cmd, insert);
        this.put = put;
        this.delete = delete;
        this.table = table;
    }

    public StoreFieldManager(ObjectProvider op, Put put, Delete delete, boolean insert, Table table)
    {
        super(op, insert);

        this.put = put;
        this.delete = delete;
        this.table = table;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        return table.getMemberColumnMappingForMember(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
    }

    protected String getFamilyName(int fieldNumber)
    {
        Column col = getColumnMapping(fieldNumber).getColumn(0); // TODO Multi column mapping?
        return HBaseUtils.getFamilyNameForColumn(col);
    }

    protected String getQualifierName(int fieldNumber)
    {
        Column col = getColumnMapping(fieldNumber).getColumn(0); // TODO Multi column mapping?
        return HBaseUtils.getQualifierNameForColumn(col);
    }

    public void storeBooleanField(int fieldNumber, boolean value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        storeBooleanInternal(familyName, qualifName, value, false);
    }

    public void storeByteField(int fieldNumber, byte value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        storeByteInternal(familyName, qualifName, value, false);
    }

    public void storeCharField(int fieldNumber, char value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        storeCharInternal(familyName, qualifName, value, false);
    }

    public void storeDoubleField(int fieldNumber, double value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        storeDoubleInternal(familyName, qualifName, value, false);
    }

    public void storeFloatField(int fieldNumber, float value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        storeFloatInternal(familyName, qualifName, value, false);
    }

    public void storeIntField(int fieldNumber, int value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        storeIntInternal(familyName, qualifName, value, false);
    }

    public void storeLongField(int fieldNumber, long value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        storeLongInternal(familyName, qualifName, value, false);
    }

    public void storeShortField(int fieldNumber, short value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        String familyName = getFamilyName(fieldNumber);
        String qualifName = getQualifierName(fieldNumber);
        storeShortInternal(familyName, qualifName, value, false);
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
            // TODO This needs to cater for embedded subclass case
            // writeObjectField(familyName, qualifName, value);
            put.add(familyName.getBytes(), qualifName.getBytes(), value.getBytes());
        }
    }

    public void storeObjectField(int fieldNumber, Object value)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (!isStorable(mmd))
        {
            return;
        }

        RelationType relationType = mmd.getRelationType(clr);
        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
        {
            // Embedded field
            if (RelationType.isRelationSingleValued(relationType))
            {
                // Embedded PC
                AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                int[] embMmdPosns = embCmd.getAllMemberPositions();
                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
                embMmds.add(mmd);
                if (value == null)
                {
                    StoreEmbeddedFieldManager storeEmbFM = new StoreEmbeddedFieldManager(ec, embCmd, put, delete, insert, embMmds, table);
                    for (int i = 0; i < embMmdPosns.length; i++)
                    {
                        AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(embMmdPosns[i]);
                        if (String.class.isAssignableFrom(embMmd.getType()) || embMmd.getType().isPrimitive() || ClassUtils.isPrimitiveWrapperType(mmd.getTypeName()))
                        {
                            // Remove property for any primitive/wrapper/String fields
                            List<AbstractMemberMetaData> colEmbMmds = new ArrayList<AbstractMemberMetaData>(embMmds);
                            colEmbMmds.add(embMmd);
                            MemberColumnMapping mapping = table.getMemberColumnMappingForEmbeddedMember(colEmbMmds);
                            Column col = mapping.getColumn(0);
                            String colFamName = HBaseUtils.getFamilyNameForColumn(col);
                            String colQualName = HBaseUtils.getQualifierNameForColumn(col);
                            delete.deleteColumn(colFamName.getBytes(), colQualName.getBytes());
                        }
                        else if (Object.class.isAssignableFrom(embMmd.getType()))
                        {
                            storeEmbFM.storeObjectField(embMmdPosns[i], null);
                        }
                    }
                    return;
                }

                ObjectProvider embOP = ec.findObjectProviderForEmbedded(value, op, mmd);
                FieldManager ffm = new StoreEmbeddedFieldManager(embOP, put, delete, insert, embMmds, table);
                embOP.provideFields(embCmd.getAllMemberPositions(), ffm);
                return;
            }
            else
            {
                // Embedded collection/map/array
                throw new NucleusUserException("Field " + mmd.getFullFieldName() + " specified as embedded but field of this type not suppported. Mark as not persistent? or not embedded?");
            }
        }

        storeNonEmbeddedObjectField(mmd, relationType, clr, value);
    }

    protected void storeNonEmbeddedObjectField(AbstractMemberMetaData mmd, RelationType relationType, ClassLoaderResolver clr, Object value)
    {
        int fieldNumber = mmd.getAbsoluteFieldNumber();
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
                            storeBooleanInternal(familyName, qualifName, (Boolean)value, mmd.isSerialized());
                            return;
                        }
                        else if (datastoreType == Double.class)
                        {
                            storeDoubleInternal(familyName, qualifName, (Double)value, mmd.isSerialized());
                            return;
                        }
                        else if (datastoreType == Integer.class)
                        {
                            storeIntInternal(familyName, qualifName, (Integer)value, mmd.isSerialized());
                            return;
                        }
                        else if (datastoreType == Long.class)
                        {
                            storeLongInternal(familyName, qualifName, (Long)value, mmd.isSerialized());
                            return;
                        }
                    }
                    if (Boolean.class.isAssignableFrom(value.getClass()))
                    {
                        storeBooleanInternal(familyName, qualifName, (Boolean)value, mmd.isSerialized());
                        return;
                    }
                    else if (Byte.class.isAssignableFrom(value.getClass()))
                    {
                        storeByteInternal(familyName, qualifName, (Byte)value, mmd.isSerialized());
                        return;
                    }
                    else if (Character.class.isAssignableFrom(value.getClass()))
                    {
                        storeCharInternal(familyName, qualifName, (Character)value, mmd.isSerialized());
                        return;
                    }
                    else if (Double.class.isAssignableFrom(value.getClass()))
                    {
                        storeDoubleInternal(familyName, qualifName, (Double)value, mmd.isSerialized());
                        return;
                    }
                    else if (Float.class.isAssignableFrom(value.getClass()))
                    {
                        storeFloatInternal(familyName, qualifName, (Float)value, mmd.isSerialized());
                        return;
                    }
                    else if (Integer.class.isAssignableFrom(value.getClass()))
                    {
                        storeIntInternal(familyName, qualifName, (Integer)value, mmd.isSerialized());
                        return;
                    }
                    else if (Long.class.isAssignableFrom(value.getClass()))
                    {
                        storeLongInternal(familyName, qualifName, (Long)value, mmd.isSerialized());
                        return;
                    }
                    else if (Short.class.isAssignableFrom(value.getClass()))
                    {
                        storeShortInternal(familyName, qualifName, (Short)value, mmd.isSerialized());
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

    private void storeBooleanInternal(String familyName, String columnName, boolean value, boolean serialised)
    {
        if (serialised)
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

    private void storeByteInternal(String familyName, String columnName, byte value, boolean serialised)
    {
        put.add(familyName.getBytes(), columnName.getBytes(), new byte[]{value});
    }

    private void storeCharInternal(String familyName, String columnName, char value, boolean serialised)
    {
        if (serialised)
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

    private void storeDoubleInternal(String familyName, String columnName, double value, boolean serialised)
    {
        if (serialised)
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

    private void storeFloatInternal(String familyName, String columnName, float value, boolean serialised)
    {
        if (serialised)
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

    private void storeIntInternal(String familyName, String columnName, int value, boolean serialised)
    {
        if (serialised)
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

    private void storeLongInternal(String familyName, String columnName, long value, boolean serialised)
    {
        if (serialised)
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

    private void storeShortInternal(String familyName, String columnName, short value, boolean serialised)
    {
        if (serialised)
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