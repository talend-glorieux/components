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
import org.talend.components.api.component.runtime.SimpleRuntimeInfo;
import ${packageTalend}.common.dataset.DatasetDefinition;
import org.talend.daikon.SimpleNamedThing;

public class ${componentClass}DatasetDefinition extends SimpleNamedThing implements DatasetDefinition<${componentClass}DatasetProperties> {

    public static final String RUNTIME_${runtimeVersion} = "org.talend.components.${componentName}.runtime_${runtimeVersion}.DatasetRuntime";

    public ${componentClass}DatasetDefinition() {
        super(${componentClass}ComponentFamilyDefinition.NAME);
    }
    
    public ${componentClass}DatasetProperties createProperties() {
        return new ${componentClass}DatasetProperties(${componentClass}ComponentFamilyDefinition.NAME);
    }

    public RuntimeInfo getRuntimeInfo(${componentClass}DatasetProperties properties, Object ctx) {
        if(properties.datastore.version.getValue() == ${componentClass}DatastoreProperties.${componentClass}Version.V_${runtimeVersion}){
            return new SimpleRuntimeInfo(this.getClass().getClassLoader(),
                    DependenciesReader.computeDependenciesFilePath("org.talend.components", "components-${componentName}/${componentName}-runtime_${runtimeVersion}"),
                    RUNTIME_${runtimeVersio});
        }
        return null;
    }
}
