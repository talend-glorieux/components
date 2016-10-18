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

package ${package}.output;

    import org.junit.Test;
    import ${packageTalend}.api.component.ConnectorTopology;
    import ${packageTalend}.api.component.runtime.RuntimeInfo;

    import java.util.Set;

    import static org.hamcrest.MatcherAssert.assertThat;
    import static org.hamcrest.Matchers.equalTo;
    import static org.junit.Assert.assertEquals;
    import static org.junit.Assert.assertTrue;

    public class ${componentClass}InputDefinitionTest {
    private final ${componentClass}InputDefinition outputDefinition = new ${componentClass}InputDefinition();

    /**
     * Check {@link ${componentClass}OutputDefinition#getSupportedConnectorTopologies()} returns ConnectorTopology.INCOMING
     */
    @Test
    public void testGetRuntimeInfo(){
            RuntimeInfo runtimeInfo = outputDefinition.getRuntimeInfo(null, null);
            assertEquals("${package}.runtime_${runtimeVersion}.${componentClass}Source", runtimeInfo.getRuntimeClassName());
            }

    /**
     * Check {@link ${componentClass}OutputDefinition#getPropertyClass()} returns class, which canonical name is
     * "${package}.output.${componentClass}OutputProperties"
     */
    @Test
    public void testGetPropertyClass() {
            Class<?> propertyClass = outputDefinition.getPropertyClass();
            String canonicalName = propertyClass.getCanonicalName();
            assertThat(canonicalName, equalTo("${package}.${componentName}.output.${componentClass}OutputProperties"));
            }
    /**
     * Check {@link ${componentClass}OutputDefinition#getName()} returns "t${componentClass}Output"
     */
    @Test
    public void testGetName() {
            String componentName = outputDefinition.getName();
            assertEquals(componentName, "t${componentClass}Output");
            }

    /**
     * Check {@link ${componentClass}OutputDefinition#getSupportedConnectorTopologies()} returns ConnectorTopology.INCOMING
     */
    @Test
    public void testGetSupportedConnectorTopologies(){
            Set<ConnectorTopology> test = outputDefinition.getSupportedConnectorTopologies();
            assertTrue(test.contains(ConnectorTopology.INCOMING));
            }
    }
