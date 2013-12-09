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

import org.datanucleus.store.hbase.metadata.MetaDataExtension;

import junit.framework.TestCase;

/**
 *
 */
public class MetaDataExtensionTest extends TestCase
{
    public void testParse()
    {
        MetaDataExtension found = MetaDataExtension.parse(MetaDataExtension.BLOOM_FILTER.name());
        assertEquals(MetaDataExtension.BLOOM_FILTER, found);
    }

    public void testInvalidConditions()
    {
        MetaDataExtension found = MetaDataExtension.parse(null);
        assertNull(found);

        found = MetaDataExtension.parse("invalid string");
        assertNull(found);
    }
}