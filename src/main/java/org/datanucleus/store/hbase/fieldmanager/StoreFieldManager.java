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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.exceptions.ReachableObjectNotCascadedException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.fieldmanager.AbstractStoreFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.hbase.HBaseStoreManager;
import org.datanucleus.store.hbase.HBaseUtils;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.EnumConversionHelper;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

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

    public StoreFieldManager(DNStateManager sm, Put put, Delete delete, boolean insert, Table table)
    {
        super(sm, insert);

        this.put = put;
        this.delete = delete;
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

    public void storeBooleanField(int fieldNumber, boolean value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        storeBooleanInternal(HBaseUtils.getFamilyNameForColumn(col), HBaseUtils.getQualifierNameForColumn(col), value, mmd.isSerialized());
    }

    public void storeByteField(int fieldNumber, byte value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        storeByteInternal(HBaseUtils.getFamilyNameForColumn(col), HBaseUtils.getQualifierNameForColumn(col), value, mmd.isSerialized());
    }

    public void storeCharField(int fieldNumber, char value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        storeCharInternal(HBaseUtils.getFamilyNameForColumn(col), HBaseUtils.getQualifierNameForColumn(col), value, mmd.isSerialized());
    }

    public void storeDoubleField(int fieldNumber, double value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        storeDoubleInternal(HBaseUtils.getFamilyNameForColumn(col), HBaseUtils.getQualifierNameForColumn(col), value, mmd.isSerialized());
    }

    public void storeFloatField(int fieldNumber, float value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        storeFloatInternal(HBaseUtils.getFamilyNameForColumn(col), HBaseUtils.getQualifierNameForColumn(col), value, mmd.isSerialized());
    }

    public void storeIntField(int fieldNumber, int value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        storeIntInternal(HBaseUtils.getFamilyNameForColumn(col), HBaseUtils.getQualifierNameForColumn(col), value, mmd.isSerialized());
    }

    public void storeLongField(int fieldNumber, long value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        storeLongInternal(HBaseUtils.getFamilyNameForColumn(col), HBaseUtils.getQualifierNameForColumn(col), value, mmd.isSerialized());
    }

    public void storeShortField(int fieldNumber, short value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        storeShortInternal(HBaseUtils.getFamilyNameForColumn(col), HBaseUtils.getQualifierNameForColumn(col), value, mmd.isSerialized());
    }

    public void storeStringField(int fieldNumber, String value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        Column col = getColumnMapping(fieldNumber).getColumn(0);
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        if (value == null)
        {
            delete.addColumn(HBaseUtils.getFamilyNameForColumn(col).getBytes(), HBaseUtils.getQualifierNameForColumn(col).getBytes());
        }
        else
        {
            if (mmd.isSerialized())
            {
                writeObjectField(HBaseUtils.getFamilyNameForColumn(col), HBaseUtils.getQualifierNameForColumn(col), value);
            }
            else
            {
                put.addColumn(HBaseUtils.getFamilyNameForColumn(col).getBytes(), HBaseUtils.getQualifierNameForColumn(col).getBytes(), value.getBytes());
            }
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
                if (!mmd.isCascadePersist())
                {
                    if (!ec.getApiAdapter().isDetached(value) && !ec.getApiAdapter().isPersistent(value))
                    {
                        // Related PC object not persistent, but cant do cascade-persist so throw exception
                        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                        {
                            NucleusLogger.PERSISTENCE.debug(Localiser.msg("007006", mmd.getFullFieldName()));
                        }
                        throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), value);
                    }
                }

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
                            delete.addColumn(colFamName.getBytes(), colQualName.getBytes());
                        }
                        else if (Object.class.isAssignableFrom(embMmd.getType()))
                        {
                            storeEmbFM.storeObjectField(embMmdPosns[i], null);
                        }
                    }
                    return;
                }

                DNStateManager embSM = ec.findStateManagerForEmbedded(value, sm, mmd, null);
                FieldManager ffm = new StoreEmbeddedFieldManager(embSM, put, delete, insert, embMmds, table);
                embSM.provideFields(embCmd.getAllMemberPositions(), ffm);
                return;
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                // TODO Support embedded collection into join table
                if (value == null)
                {
                    
                }
            }

            // Embedded collection/map/array
            throw new NucleusUserException("Field " + mmd.getFullFieldName() + " specified as embedded but field of this type not suppported. Mark as not persistent? or not embedded?");
        }

        storeNonEmbeddedObjectField(mmd, relationType, clr, value);
    }

    protected void storeNonEmbeddedObjectField(AbstractMemberMetaData mmd, RelationType relationType, ClassLoaderResolver clr, Object value)
    {
        int fieldNumber = mmd.getAbsoluteFieldNumber();
        MemberColumnMapping mapping = getColumnMapping(fieldNumber);

        if (value instanceof Optional)
        {
            if (relationType != RelationType.NONE)
            {
                relationType = RelationType.ONE_TO_ONE_UNI;
            }

            Optional opt = (Optional)value;
            if (opt.isPresent())
            {
                value = opt.get();
            }
            else
            {
                value = null;
            }
        }

        if (value == null)
        {
            // Null will omit the column(s)
            for (int i=0;i<mapping.getNumberOfColumns();i++)
            {
                Column col = mapping.getColumn(i);
                delete.addColumn(HBaseUtils.getFamilyNameForColumn(col).getBytes(), HBaseUtils.getQualifierNameForColumn(col).getBytes());
            }
            return;
        }

        String relationStorageMode = ec.getStoreManager().getStringProperty(PropertyNames.PROPERTY_RELATION_IDENTITY_STORAGE_MODE);
        if (RelationType.isRelationSingleValued(relationType))
        {
            // 1-1/N-1 relation
            if (!mmd.isCascadePersist())
            {
                if (!ec.getApiAdapter().isDetached(value) && !ec.getApiAdapter().isPersistent(value))
                {
                    // Related PC object not persistent, but cant do cascade-persist so throw exception
                    if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                    {
                        NucleusLogger.PERSISTENCE.debug(Localiser.msg("007006", mmd.getFullFieldName()));
                    }
                    throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), value);
                }
            }

            Column col = mapping.getColumn(0);
            String familyName = HBaseUtils.getFamilyNameForColumn(col);
            String qualifName = HBaseUtils.getQualifierNameForColumn(col);

            if (mmd.isSerialized())
            {
                // Assign an StateManager to the serialised object if none present
                DNStateManager pcSM = ec.findStateManager(value);
                if (pcSM == null || ec.getApiAdapter().getExecutionContext(value) == null)
                {
                    pcSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, value, false, sm, fieldNumber, null);
                }

                if (pcSM != null)
                {
                    pcSM.setStoringPC();
                }

                // Persist member as serialised
                writeObjectField(familyName, qualifName, value);

                if (pcSM != null)
                {
                    pcSM.unsetStoringPC();
                }
                return;
            }

            // Persist identity in the column of this object
            Object valuePC = ec.persistObjectInternal(value, sm, fieldNumber, -1);
            Object valueID = ec.getApiAdapter().getIdForObject(valuePC);

            if (relationStorageMode.equalsIgnoreCase(StoreManager.RELATION_IDENTITY_STORAGE_PERSISTABLE_IDENTITY))
            {
                writeObjectField(familyName, qualifName, IdentityUtils.getPersistableIdentityForId(valueID));
            }
            else if (relationStorageMode.equalsIgnoreCase(HBaseStoreManager.RELATION_IDENTITY_STORAGE_HBASE_LEGACY))
            {
                // Legacy
                writeObjectField(familyName, qualifName, valueID);
            }
            else
            {
                throw new NucleusUserException("This store plugin does not support relation identity storageMode of " + relationStorageMode);
            }
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            // 1-N/M-N relation
            Column col = mapping.getColumn(0);
            String familyName = HBaseUtils.getFamilyNameForColumn(col);
            String qualifName = HBaseUtils.getQualifierNameForColumn(col);

            if (mmd.isSerialized())
            {
                // Persist member as serialised
                writeObjectField(familyName, qualifName, value);
                SCOUtils.wrapSCOField(sm, fieldNumber, value, true);
                return;
            }

            // Collection/Map/Array
            if (mmd.hasCollection())
            {
                Collection coll = (Collection)value;
                if (!mmd.isCascadePersist())
                {
                    // Field doesnt support cascade-persist so no reachability
                    if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                    {
                        NucleusLogger.PERSISTENCE.debug(Localiser.msg("007006", mmd.getFullFieldName()));
                    }

                    // Check for any persistable elements that aren't persistent
                    for (Object element : coll)
                    {
                        if (!ec.getApiAdapter().isDetached(element) && !ec.getApiAdapter().isPersistent(element))
                        {
                            // Element is not persistent so throw exception
                            throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), element);
                        }
                    }
                }

                Collection collIds = new ArrayList();
                Iterator collIter = coll.iterator();
                while (collIter.hasNext())
                {
                    Object element = collIter.next();
                    if (element != null)
                    {
                        Object elementPC = ec.persistObjectInternal(element, sm, fieldNumber, -1);
                        Object elementID = ec.getApiAdapter().getIdForObject(elementPC);
                        if (relationStorageMode.equalsIgnoreCase(StoreManager.RELATION_IDENTITY_STORAGE_PERSISTABLE_IDENTITY))
                        {
                            collIds.add(IdentityUtils.getPersistableIdentityForId(elementID));
                        }
                        else if (relationStorageMode.equalsIgnoreCase(HBaseStoreManager.RELATION_IDENTITY_STORAGE_HBASE_LEGACY))
                        {
                            // Legacy
                            collIds.add(elementID);
                        }
                    }
                    else
                    {
                        if (relationStorageMode.equalsIgnoreCase(StoreManager.RELATION_IDENTITY_STORAGE_PERSISTABLE_IDENTITY))
                        {
                            collIds.add("NULL");
                        }
                        else if (relationStorageMode.equalsIgnoreCase(HBaseStoreManager.RELATION_IDENTITY_STORAGE_HBASE_LEGACY))
                        {
                            collIds.addAll(null);
                        }
                    }
                }

                if (mmd.getCollection().isSerializedElement())
                {
                    // TODO Support this
                    throw new NucleusException("Don't currently support serialized collection elements (field=" + mmd.getFullFieldName() + ")");
                }

                // Persist list<ids> into the column of this object
                writeObjectField(familyName, qualifName, collIds);
                SCOUtils.wrapSCOField(sm, fieldNumber, value, true);
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
                        Object pKey = ec.persistObjectInternal(mapKey, sm, fieldNumber, -1);
                        mapKey = ec.getApiAdapter().getIdForObject(pKey);
                        if (relationStorageMode.equalsIgnoreCase(StoreManager.RELATION_IDENTITY_STORAGE_PERSISTABLE_IDENTITY))
                        {
                            mapKey = IdentityUtils.getPersistableIdentityForId(mapKey);
                        }
                    }
                    if (ec.getApiAdapter().isPersistable(mapValue))
                    {
                        if (mapValue != null)
                        {
                            Object pVal = ec.persistObjectInternal(mapValue, sm, fieldNumber, -1);
                            mapValue = ec.getApiAdapter().getIdForObject(pVal);
                            if (relationStorageMode.equalsIgnoreCase(StoreManager.RELATION_IDENTITY_STORAGE_PERSISTABLE_IDENTITY))
                            {
                                mapValue = IdentityUtils.getPersistableIdentityForId(mapValue);
                            }
                        }
                        else
                        {
                            mapValue = "NULL";
                        }
                    }
                    if (mapValue == null)
                    {
                        mapValue = "NULL";
                    }
                    mapIds.put(mapKey, mapValue);
                }

                if (mmd.getMap().isSerializedKey() || mmd.getMap().isSerializedValue())
                {
                    // TODO Support this
                    throw new NucleusException("Don't currently support serialized map keys/values (field=" + mmd.getFullFieldName() + ")");
                }

                // Persist map<keyids,valids> into the column of this object
                writeObjectField(familyName, qualifName, mapIds);
                SCOUtils.wrapSCOField(sm, fieldNumber, value, true);
            }
            else if (mmd.hasArray())
            {
                Collection arrIds = new ArrayList();
                for (int i=0;i<Array.getLength(value);i++)
                {
                    Object element = Array.get(value, i);
                    if (element != null)
                    {
                        Object elementPC = ec.persistObjectInternal(element, sm, fieldNumber, -1);
                        Object elementID = ec.getApiAdapter().getIdForObject(elementPC);
                        if (relationStorageMode.equalsIgnoreCase(StoreManager.RELATION_IDENTITY_STORAGE_PERSISTABLE_IDENTITY))
                        {
                            arrIds.add(IdentityUtils.getPersistableIdentityForId(elementID));
                        }
                        else if (relationStorageMode.equalsIgnoreCase(HBaseStoreManager.RELATION_IDENTITY_STORAGE_HBASE_LEGACY))
                        {
                            arrIds.add(elementID);
                        }
                    }
                    else
                    {
                        if (relationStorageMode.equalsIgnoreCase(StoreManager.RELATION_IDENTITY_STORAGE_PERSISTABLE_IDENTITY))
                        {
                            arrIds.add("NULL");
                        }
                        else if (relationStorageMode.equalsIgnoreCase(HBaseStoreManager.RELATION_IDENTITY_STORAGE_HBASE_LEGACY))
                        {
                            arrIds.add(null);
                        }
                    }
                }

                if (mmd.getArray().isSerializedElement())
                {
                    // TODO Support this
                    throw new NucleusException("Don't currently support serialized array elements (field=" + mmd.getFullFieldName() + ")");
                }

                // Persist list of array element ids into the column of this object
                writeObjectField(familyName, qualifName, arrIds);
            }
        }
        else
        {
            if (mmd.isSerialized())
            {
                // Persist member as serialised
                Column col = mapping.getColumn(0);
                String familyName = HBaseUtils.getFamilyNameForColumn(col);
                String qualifName = HBaseUtils.getQualifierNameForColumn(col);

                writeObjectField(familyName, qualifName, value);
                SCOUtils.wrapSCOField(sm, fieldNumber, value, true);
                return;
            }

            if (mapping.getTypeConverter() != null)
            {
                // Persist using the provided converter
                Object datastoreValue = mapping.getTypeConverter().toDatastoreType(value);
                if (mapping.getNumberOfColumns() > 1)
                {
                    // Multicolumn, so add value for each column
                    for (int i=0;i<mapping.getNumberOfColumns();i++)
                    {
                        Column col = mapping.getColumn(i);
                        Object colValue = Array.get(datastoreValue, i);
                        byte[] valueColBytes = getPersistableBytesForObject(col, colValue);
                        // TODO What if valueColBytes is null, provide mechanism to serialise?
                        put.addColumn(HBaseUtils.getFamilyNameForColumn(col).getBytes(), HBaseUtils.getQualifierNameForColumn(col).getBytes(), valueColBytes);
                    }
                }
                else
                {
                    Column col = mapping.getColumn(0);
                    byte[] valueColBytes = getPersistableBytesForObject(col, datastoreValue);
                    put.addColumn(HBaseUtils.getFamilyNameForColumn(col).getBytes(), HBaseUtils.getQualifierNameForColumn(col).getBytes(), valueColBytes);
                }
                return;
            }

            Column col = mapping.getColumn(0);
            String familyName = HBaseUtils.getFamilyNameForColumn(col);
            String qualifName = HBaseUtils.getQualifierNameForColumn(col);

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
                Object storedValue = EnumConversionHelper.getStoredValueFromEnum(mmd, FieldRole.ROLE_FIELD, (Enum)value);
                if (storedValue instanceof Number)
                {
                    put.addColumn(familyName.getBytes(), qualifName.getBytes(), Bytes.toBytes(((Number)storedValue).intValue()));
                }
                else
                {
                    put.addColumn(familyName.getBytes(), qualifName.getBytes(), ((String)storedValue).getBytes());
                }
                return;
            }
            else if (String.class.isAssignableFrom(value.getClass()))
            {
                put.addColumn(familyName.getBytes(), qualifName.getBytes(), ((String)value).getBytes());
                return;
            }
            else if (Collection.class.isAssignableFrom(value.getClass()))
            {
                Collection coll = (Collection) value;
                if (coll.isEmpty())
                {
                    writeObjectField(familyName, qualifName, value);
                    SCOUtils.wrapSCOField(sm, fieldNumber, value, true);
                    return;
                }

                Collection dbColl = null;
                if (value instanceof List || value instanceof Queue)
                {
                    dbColl = new ArrayList();
                }
                else
                {
                    dbColl = new HashSet();
                }

                TypeConverter elemConv = mapping.getTypeConverterForComponent(FieldRole.ROLE_COLLECTION_ELEMENT);

                Iterator collIter = coll.iterator();
                while (collIter.hasNext())
                {
                    Object element = collIter.next();
                    Object datastoreValue = element;
                    if (elemConv != null)
                    {
                        datastoreValue = elemConv.toDatastoreType(element);
                    }
                    dbColl.add(datastoreValue);
                }
                writeObjectField(familyName, qualifName, dbColl);
                SCOUtils.wrapSCOField(sm, fieldNumber, value, true);
                return;
            }
            else if (Map.class.isAssignableFrom(value.getClass()))
            {
                Map map = (Map) value;
                if (map.isEmpty())
                {
                    writeObjectField(familyName, qualifName, value);
                    SCOUtils.wrapSCOField(sm, fieldNumber, value, true);
                    return;
                }

                TypeConverter keyConv = mapping.getTypeConverterForComponent(FieldRole.ROLE_MAP_KEY);
                TypeConverter valConv = mapping.getTypeConverterForComponent(FieldRole.ROLE_MAP_VALUE);

                Iterator<Map.Entry> entryIter = map.entrySet().iterator();
                Map dbMap = new HashMap();
                while (entryIter.hasNext())
                {
                    Map.Entry entry = entryIter.next();

                    Object key = entry.getKey();
                    Object datastoreKey = key;
                    if (keyConv != null)
                    {
                        datastoreKey = keyConv.toDatastoreType(key);
                    }

                    Object val = entry.getValue();
                    Object datastoreVal = val;
                    if (valConv != null)
                    {
                        datastoreVal = valConv.toDatastoreType(val);
                    }

                    dbMap.put(datastoreKey, datastoreVal);
                }
                writeObjectField(familyName, qualifName, dbMap);
                SCOUtils.wrapSCOField(sm, fieldNumber, value, true);
                return;
            }
            else if (value.getClass().isArray())
            {
                // TODO Add handling for this
            }

            // Fallback to built-in String converters
            // TODO Make use of default TypeConverter for a type before falling back to String/Long
            TypeConverter strConv = ec.getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
            if (strConv != null)
            {
                // Persist as a String
                String strValue = (String)strConv.toDatastoreType(value);
                put.addColumn(familyName.getBytes(), qualifName.getBytes(), strValue.getBytes());
                return;
            }

            // Fallback to serialised
            writeObjectField(familyName, qualifName, value);
            SCOUtils.wrapSCOField(sm, fieldNumber, value, true);
        }
    }

    protected void writeObjectField(String familyName, String columnName, Object value)
    {
        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(value);
            put.addColumn(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
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
                put.addColumn(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
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
            put.addColumn(familyName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
        }
    }

    private void storeByteInternal(String familyName, String columnName, byte value, boolean serialised)
    {
        put.addColumn(familyName.getBytes(), columnName.getBytes(), new byte[]{value});
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
                put.addColumn(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
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
            put.addColumn(familyName.getBytes(), columnName.getBytes(), ("" + value).getBytes());
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
                put.addColumn(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
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
            put.addColumn(familyName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
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
                put.addColumn(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
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
            put.addColumn(familyName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
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
                put.addColumn(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
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
            put.addColumn(familyName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
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
                put.addColumn(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
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
            put.addColumn(familyName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
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
                put.addColumn(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
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
            put.addColumn(familyName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
        }
    }

    protected byte[] getPersistableBytesForObject(Column col, Object value)
    {
        if (value == null)
        {
            return null;
        }
        else if (value.getClass() == byte[].class)
        {
            return (byte[]) value;
        }

        if (Boolean.class.isAssignableFrom(value.getClass()))
        {
            return Bytes.toBytes((Boolean)value);
        }
        else if (Byte.class.isAssignableFrom(value.getClass()))
        {
            return new byte[]{(Byte) value};
        }
        else if (Character.class.isAssignableFrom(value.getClass()))
        {
            return ("" + value).getBytes();
        }
        else if (Double.class.isAssignableFrom(value.getClass()))
        {
            return Bytes.toBytes((Double)value);
        }
        else if (Float.class.isAssignableFrom(value.getClass()))
        {
            return Bytes.toBytes((Float)value);
        }
        else if (Integer.class.isAssignableFrom(value.getClass()))
        {
            return Bytes.toBytes((Integer)value);
        }
        else if (Long.class.isAssignableFrom(value.getClass()))
        {
            return Bytes.toBytes((Long)value);
        }
        else if (Short.class.isAssignableFrom(value.getClass()))
        {
            return Bytes.toBytes((Short)value);
        }
        else if (BigDecimal.class.isAssignableFrom(value.getClass()))
        {
            return Bytes.toBytes((BigDecimal)value);
        }
        else if (String.class.isAssignableFrom(value.getClass()))
        {
            return ((String)value).getBytes();
        }
        else if (Enum.class.isAssignableFrom(value.getClass()))
        {
            ColumnMetaData colmd = col.getColumnMetaData();
            if (MetaDataUtils.persistColumnAsNumeric(colmd))
            {
                return Bytes.toBytes(((Enum)value).ordinal());
            }

            return ((Enum)value).name().getBytes();
        }
        else if (java.util.Date.class.isAssignableFrom(value.getClass()))
        {
            TypeConverter<java.util.Date, String> dateStrConv = ec.getTypeManager().getTypeConverterForType(value.getClass(), String.class);
            String datastoreVal = dateStrConv.toDatastoreType((java.util.Date) value);
            return datastoreVal.getBytes();
        }
        else
        {
            NucleusLogger.PERSISTENCE.warn("Persistence of column " + col + " has value of type " + value.getClass().getName() + " but no conversion to bytes defined. Report this");
        }
        return null;
    }
}