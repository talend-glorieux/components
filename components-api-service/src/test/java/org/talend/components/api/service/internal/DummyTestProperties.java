package org.talend.components.api.service.internal;

import org.talend.components.api.properties.ComponentPropertiesImpl;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.property.PropertyFactory;

/**
 * Dummy test properties used for the unit / integration tests.
 */
public class DummyTestProperties extends ComponentPropertiesImpl {

    private Property<String> firstName = PropertyFactory.newString("first name").setRequired();
    private Property<String> lastName = PropertyFactory.newString("last name").setRequired();
    private Property<String> dateOfBirth = PropertyFactory.newString("date of birth").setRequired();

    /**
     * Default empty properties.
     */
    public DummyTestProperties() {
        super("dummy properties for test purpose");
    }

    @Override
    public void setupLayout() {
        super.setupLayout();
        Form form = getForm(Form.MAIN);
        form.addRow(firstName);
        form.addRow(lastName);
        form.addRow(dateOfBirth);
    }

}
