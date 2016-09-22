package org.talend.components.api.service.util;

import org.talend.daikon.NamedThing;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;

public class JsonWidget {
    private Widget widget;
    private Form form;

    public JsonWidget(Widget widget, Form form) {
        this.widget = widget;
        this.form = form;
    }

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
