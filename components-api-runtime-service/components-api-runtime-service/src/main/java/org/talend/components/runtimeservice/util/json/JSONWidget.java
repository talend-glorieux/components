package org.talend.components.runtimeservice.util.json;

import org.talend.daikon.NamedThing;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;

/**
 * Wrapper of Widget
 */
public class JSONWidget {
    private Widget widget;
    private Form form;

    public JSONWidget(Widget widget, Form form) {
        this.widget = widget;
        this.form = form;
    }

    //UISchema do not have columns in same line, so have to make them sequence
    public int getOrder() {
        int base = 100;
        if (form.getName().equals(Form.ADVANCED)) {
            base = 10000;
        }
        return widget.getRow() * base + widget.getOrder();
    }

    public NamedThing getContent() {
        return widget.getContent();
    }

    public String getName() {
        if (getContent() instanceof Form) {
            return ((Form) getContent()).getProperties().getName();
        }
        return getContent().getName();
    }

    public Widget getWidget() {
        return widget;
    }
}
