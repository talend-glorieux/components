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

package ${package}.input;

import org.junit.Test;
import ${packageTalend}.api.component.ConnectorTopology;
import ${packageTalend}.api.component.runtime.RuntimeInfo;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ${componentClass}InputDefinitionTest {
    private final ${componentClass}InputDefinition inputDefinition = new ${componentClass}InputDefinition();

    /**
     * Check {@link ${componentClass}InputDefinition#getRuntimeInfo(Properties properties, ConnectorTopology connectorTopology)} returns RuntimeInfo,
     * which runtime class name is "${package}.runtime_${runtimeVersion}.JmsSink"
     */
    @Test
    public void testGetRuntimeInfo(){
        RuntimeInfo runtimeInfo = inputDefinition.getRuntimeInfo(null, null);
        assertEquals("${package}.runtime_${runtimeVersion}.${componentClass}Sink", runtimeInfo.getRuntimeClassName());
    }

    /**
     * Check {@link ${componentClass}InputDefinition#getPropertyClass()} returns class, which canonical name is
     * "${package}.${componentName}.input.${componentClass}InputProperties"
     */
    @Test
    public void testGetPropertyClass() {
        Class<?> propertyClass = inputDefinition.getPropertyClass();
        String canonicalName = propertyClass.getCanonicalName();
        assertThat(canonicalName, equalTo("${package}.${componentName}.input.${componentClass}InputProperties"));
    }
    /**
     * Check {@link ${componentClass}InputDefinition#getName()} returns "t${componentClass}Input"
     */
    @Test
    public void testGetName() {
        String componentName = inputDefinition.getName();
        assertEquals(componentName, "t${componentClass}Input");
    }

    /**
     * Check {@link ${componentClass}InputDefinition#getSupportedConnectorTopologies()} returns ConnectorTopology.OUTGOING
     */
    @Test
    public void testGetSupportedConnectorTopologies(){
        Set<ConnectorTopology> test = inputDefinition.getSupportedConnectorTopologies();
        assertTrue(test.contains(ConnectorTopology.OUTGOING));
    }
}
