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
package org.talend.components.common.dataset.runtime;

import java.io.IOException;

import org.apache.avro.Schema;
import org.talend.components.api.component.runtime.RuntimableRuntime;
import org.talend.components.api.container.RuntimeContainer;

public interface DatasetRuntime extends RuntimableRuntime {

    /**
     * Return the schema associated with the this dataset, or null if none found.
     */
    Schema getEndpointSchema(RuntimeContainer container) throws IOException;
}
