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

import ${packageTalend}.api.AbstractComponentFamilyDefinition;
import ${packageTalend}.api.ComponentInstaller;
import ${packageTalend}.api.Constants;
import ${package}.input.${componentClass}InputDefinition;
import ${package}.output.${componentClass}OutputDefinition;

import aQute.bnd.annotation.component.Component;

/**
 * Install all of the definitions provided for the ${componentName} family of components.
 */
@Component(name = Constants.COMPONENT_INSTALLER_PREFIX
        + ${componentClass}ComponentFamilyDefinition.NAME, provide = ComponentInstaller.class)
public class ${componentClass}ComponentFamilyDefinition extends AbstractComponentFamilyDefinition implements ComponentInstaller {

    public static final String NAME = "${componentName}";

    public ${componentClass}ComponentFamilyDefinition() {
        super(NAME, new ${componentClass}DatastoreDefinition(), new ${componentClass}DatasetDefinition(), new ${componentClass}InputDefinition(),
                new ${componentClass}OutputDefinition());
    }

    public void install(ComponentFrameworkContext ctx) {
        ctx.registerComponentFamilyDefinition(this);
    }
}
