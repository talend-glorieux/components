// ============================================================================
//
// Copyright (C) 2006-2016 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================

package ${package};

import org.junit.Test;

import ${packageTalend}.api.component.runtime.RuntimeInfo;

import static org.junit.Assert.assertEquals;

public class ${componentClass}DatastoreDefinitionTest {

    private final ${componentClass}DatastoreDefinition datastoreDefinition = new ${componentClass}DatastoreDefinition();

    /**
    * Check {@link ${componentClass}DatastoreDefinition#getRuntimeInfo(JmsDatasetProperties properties, Object ctx)} returns RuntimeInfo,
    * which runtime class name is "${package}.runtime_${runtimeVersion}.DatastoreRuntime"
    */
    @Test
    public void testGetRuntimeInfo(){
        RuntimeInfo runtimeInfo = datastoreDefinition.getRuntimeInfo(null, null);
        assertEquals("${package}.runtime_${runtimeVersion}.DatasetRuntime", runtimeInfo.getRuntimeClassName());
    }

    /**
    * Check {@link ${componentClass}DatastoreDefinition#createProperties()} returns ${componentClass}DatastoreProperties, which canonical name is
    * "${componentName}"
    */
    @Test
    public void testCreateProperties(){
        ${componentClass}DatastoreProperties props = datastoreDefinition.createProperties();
        assertEquals("${componentName}", props.getName());
    }
}
