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

import org.junit.Test;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;

import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertNull;

public class ${componentClass}DatasetPropertiesTest {
    /**
     * Checks {@link ${componentClass}DatasetProperties} sets correctly initial schema
     * property
     */
    @Test
    public void testDefaultProperties() {
        ${componentClass}DatasetProperties properties = new ${componentClass}DatasetProperties("test");
        assertNull(properties.main.schema.getValue());
        // FIXME add asserts to test all properties

    }

    /**
     * Checks {@link ${componentClass}DatasetProperties} sets correctly initial layout
     * properties
     */
    @Test
    public void testSetupLayout() {
        ${componentClass}DatasetProperties properties = new ${componentClass}DatasetProperties("test");
        properties.main.init();

        properties.setupLayout();

        Form main = properties.getForm(Form.MAIN);
        assertThat(main, notNullValue());

        Collection<Widget> mainWidgets = main.getWidgets();
        // FIXME add correct value for the size
        //assertThat(mainWidgets, hasSize(5));
        // FIXME add component widgets and asserts associated
    }

    /**
     * Checks {@link ${componentClass}DatasetProperties} sets correctly layout after refresh
     * properties
     */
    @Test
    public void testRefreshLayout() {
        ${componentClass}DatasetProperties properties = new ${componentClass}DatasetProperties("test");
        properties.init();
        properties.refreshLayout(properties.getForm(Form.MAIN));
        // FIXME add asserts for the layout
        // assertFalse(properties.getForm(Form.MAIN).getWidget("msgType").isHidden());
    }
}
