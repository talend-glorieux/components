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

import java.util.EnumSet;
import java.util.Set;

import ${packageTalend}.api.component.AbstractComponentDefinition;
import ${packageTalend}.api.component.ConnectorTopology;
import ${packageTalend}.api.component.runtime.DependenciesReader;
import ${packageTalend}.api.component.runtime.RuntimeInfo;
import ${packageTalend}.api.component.runtime.SimpleRuntimeInfo;
import ${packageTalend}.api.properties.ComponentProperties;
import ${packageTalend}.${componentName}.${componentClass}DatastoreProperties;

import org.talend.daikon.properties.Properties;
import org.talend.daikon.properties.property.Property;

public class ${componentClass}OutputDefinition extends AbstractComponentDefinition {

    public static final String COMPONENT_NAME = "t${componentClass}Output"; //$NON-NLS-1$

    public static final String RUNTIME_${runtimeVersion} = "org.talend.components.${componentName}.runtime_${runtimeVersion}.${componentClass}Source";

    public ${componentClass}OutputDefinition() {
        super(COMPONENT_NAME);
    }

    @Override
    public Class<? extends ComponentProperties> getPropertyClass() {
        return ${componentClass}OutputProperties.class;
    }

    public RuntimeInfo getRuntimeInfo(Properties properties, ConnectorTopology connectorTopology) {
        // TODO check to use parameters..
        ${componentClass}InputProperties ${componentName}InputProperties = new ${componentClass}InputProperties(COMPONENT_NAME);
        if(${componentName}InputProperties.dataset.datastore.version.getValue() == ${componentClass}DatastoreProperties.${componentClass}Version.V_${runtimeVersion}){
            return new SimpleRuntimeInfo(this.getClass().getClassLoader(),
                    DependenciesReader.computeDependenciesFilePath("org.talend.components", "components-${componentName}/${componentName}-runtime_${runtimeVersion}"),
                    RUNTIME_${runtimeVersion} );
        }
        return null;
    }

    public Property[] getReturnProperties() {
        return new Property[]{};
    }

    public Set<ConnectorTopology> getSupportedConnectorTopologies() {
        return EnumSet.of(ConnectorTopology.INCOMING);
    }
}
