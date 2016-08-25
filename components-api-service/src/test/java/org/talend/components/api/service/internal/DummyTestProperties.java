package org.talend.components.api.service.internal;

import org.talend.components.api.properties.ComponentPropertiesImpl;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.property.PropertyFactory;

/**
 *
 */
public class DummyTestProperties extends ComponentPropertiesImpl {

    public DummyTestProperties() {
        super("dummy properties for test purpose");
    }

    @Override
    public void setupLayout() {
        super.setupLayout();
        Form form = getForm(Form.MAIN);
        form.addRow(PropertyFactory.newString("first name").setRequired());
        form.addRow(PropertyFactory.newString("last name").setRequired());
        form.addRow(PropertyFactory.newString("date of birth").setRequired());
    }

}
