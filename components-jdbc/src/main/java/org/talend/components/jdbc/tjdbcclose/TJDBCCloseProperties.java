// ============================================================================
//
// Copyright (C) 2006-2015 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.jdbc.tjdbcclose;

import static org.talend.daikon.properties.presentation.Widget.widget;

import org.talend.components.api.properties.ComponentPropertiesImpl;
import org.talend.components.api.properties.ComponentReferenceProperties;
import org.talend.components.api.properties.ComponentReferencePropertiesEnclosing;
import org.talend.components.jdbc.CommonUtils;
import org.talend.components.jdbc.RuntimeSettingProvider;
import org.talend.components.jdbc.runtime.setting.AllSetting;
import org.talend.components.jdbc.tjdbcconnection.TJDBCConnectionDefinition;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;

public class TJDBCCloseProperties extends ComponentPropertiesImpl
        implements ComponentReferencePropertiesEnclosing, RuntimeSettingProvider {

    public TJDBCCloseProperties(String name) {
        super(name);
    }

    // main
    public ComponentReferenceProperties referencedComponent = new ComponentReferenceProperties("referencedComponent", this);

    @Override
    public void setupLayout() {
        super.setupLayout();

        Form mainForm = CommonUtils.addForm(this, Form.MAIN);

        Widget compListWidget = widget(referencedComponent).setWidgetType(Widget.COMPONENT_REFERENCE_WIDGET_TYPE);
        referencedComponent.componentType.setValue(TJDBCConnectionDefinition.COMPONENT_NAME);
        mainForm.addRow(compListWidget);
    }

    @Override
    public void afterReferencedComponent() {
        // do nothing
    }

    @Override
    public AllSetting getRuntimeSetting() {
        AllSetting setting = new AllSetting();

        setting.setReferencedComponentId(referencedComponent.componentInstanceId.getValue());
        setting.setReferencedComponentProperties(referencedComponent.componentProperties);

        return setting;
    }

}
