/**********************************************************************
Copyright (c) 2012 Nicolas Seyvet and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
   ...
**********************************************************************/
package org.datanucleus.store.hbase.metadata;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.FileMetaData;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.store.hbase.metadata.MetaDataExtensionParser;
import org.datanucleus.store.hbase.metadata.MetaDataExtension;

/**
 * comments.
 */
public class MetaDataExtensionParserTest extends TestCase
{
    static final String TEST_FAMILY = "testFamily";
    final static String TEST_BASE = MetaDataExtensionParser.BASE + TEST_FAMILY+ ".";

    public void testWithNoExtension()
    {
        MetaDataExtensionParser ep = new MetaDataExtensionParser(getPersistenceClass());
        assertFalse(ep.hasExtensions());
    }

    public void testValidExtensions()
    {
        AbstractClassMetaData acmd = getPersistenceClass();
        acmd.addExtension(MetaData.VENDOR_NAME, TEST_BASE + MetaDataExtension.BLOOM_FILTER.name(), "ROW");
        acmd.addExtension(MetaData.VENDOR_NAME, TEST_BASE + MetaDataExtension.COMPRESSION.name(), "LZO");
        acmd.addExtension(MetaData.VENDOR_NAME, TEST_BASE + MetaDataExtension.IN_MEMORY.name(), "true");
        acmd.addExtension(MetaData.VENDOR_NAME, TEST_BASE + MetaDataExtension.KEEP_DELETED_CELLS.name(), "true");
        acmd.addExtension(MetaData.VENDOR_NAME, TEST_BASE + MetaDataExtension.BLOCK_CACHE_ENABLED.name(), "false");
        acmd.addExtension(MetaData.VENDOR_NAME, TEST_BASE + MetaDataExtension.MAX_VERSIONS.name(), "5");
        acmd.addExtension(MetaData.VENDOR_NAME, TEST_BASE + MetaDataExtension.TIME_TO_LIVE.name(), "500");

        MetaDataExtensionParser ep = new MetaDataExtensionParser(acmd);

        assertTrue(ep.hasExtensions());

        HTableDescriptor descriptor = getTable();
        ep.applyExtensions(descriptor, TEST_FAMILY);

        HColumnDescriptor cf = descriptor.getColumnFamilies()[0];
        assertTrue(cf.isInMemory());
        assertFalse(cf.isBlockCacheEnabled());
        assertEquals(KeepDeletedCells.TRUE, cf.getKeepDeletedCells());
        assertEquals(5, cf.getMaxVersions());
        assertEquals(500, cf.getTimeToLive());
        assertEquals(Compression.Algorithm.LZO, cf.getCompression());
        assertEquals(BloomType.ROW, cf.getBloomFilterType());
    }

    public void testInvalidFamilyName()
    {
        AbstractClassMetaData acmd = getPersistenceClass();
        acmd.addExtension(MetaData.VENDOR_NAME, TEST_BASE + MetaDataExtension.BLOOM_FILTER.name(), "ROW");
        MetaDataExtensionParser ep = new MetaDataExtensionParser(acmd);

        HTableDescriptor descriptor = new HTableDescriptor();
        HColumnDescriptor cf = new HColumnDescriptor("invalid value");
        descriptor.addFamily(cf);
        try
        {
            ep.applyExtensions(descriptor, TEST_FAMILY);
            fail("Invalid column name.");
        }
        catch (IllegalArgumentException e)
        {

        }
    }

    private AbstractClassMetaData getPersistenceClass()
    {
        FileMetaData md = new FileMetaData();
        md.newPackageMetadata("my.test.package");
        md.getPackage(0).newClassMetadata("FakeEntity");
        return md.getPackage(0).getClass(0);
    }

    private HTableDescriptor getTable()
    {
        HTableDescriptor descriptor = new HTableDescriptor();
        HColumnDescriptor cf = new HColumnDescriptor(TEST_FAMILY);
        descriptor.addFamily(cf);
        return descriptor;
    }
}