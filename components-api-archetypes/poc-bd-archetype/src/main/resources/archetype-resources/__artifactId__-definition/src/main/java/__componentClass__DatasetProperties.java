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

import ${packageTalend}.common.dataset.DatasetProperties;
import org.talend.daikon.properties.PropertiesImpl;

import org.talend.components.common.SchemaProperties;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.property.Property;

public class ${componentClass}DatasetProperties extends PropertiesImpl implements DatasetProperties {

    public SchemaProperties main = new SchemaProperties("main");

    public ${componentClass}DatasetProperties(String name) {
        super(name);
    }

    @Override
    public void setupLayout() {
        Form mainForm = new Form(this, Form.MAIN);
        mainForm.addRow(main.getForm(Form.MAIN));
    }

    public ${componentClass}DatastoreProperties getDatastoreProperties() {
        return datastore;
    }
}
