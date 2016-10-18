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

import ${packageTalend}.api.component.runtime.DependenciesReader;
import ${packageTalend}.api.component.runtime.RuntimeInfo;
import ${packageTalend}.api.component.runtime.SimpleRuntimeInfo;
import ${packageTalend}.common.datastore.DatastoreDefinition;
import org.talend.daikon.SimpleNamedThing;

public class ${componentClass}DatastoreDefinition extends SimpleNamedThing implements DatastoreDefinition<${componentClass}DatastoreProperties> {

    public static final String RUNTIME_${runtimeVersion} = "org.talend.components.${componentName}.runtime_${runtimeVersion}.DatastoreRuntime";

    public ${componentClass}DatastoreDefinition() {
        super(${componentClass}ComponentFamilyDefinition.NAME);
    }

    @Override
    public ${componentClass}DatastoreProperties createProperties() {
        return new ${componentClass}DatastoreProperties(${componentClass}ComponentFamilyDefinition.NAME);
    }

    @Override
    public RuntimeInfo getRuntimeInfo(${componentClass}DatastoreProperties properties, Object ctx) {
        if(properties.version.getValue() == ${componentClass}DatastoreProperties.${componentClass}Version.V_${runtimeVersion}){
            return new SimpleRuntimeInfo(this.getClass().getClassLoader(),
                    DependenciesReader.computeDependenciesFilePath("org.talend.components", "components-${componentName}/${componentName}-runtime_${runtimeVersion}"),
                    RUNTIME_${runtimeVersion});
        }
        return null;
    }
}
