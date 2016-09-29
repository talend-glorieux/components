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
package org.talend.components.common.datastore.runtime;

import java.io.IOException;
import java.util.List;

import org.talend.components.api.component.runtime.RuntimableRuntime;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.daikon.NamedThing;
import org.talend.daikon.properties.ValidationResult;

public interface DatastoreRuntime extends RuntimableRuntime {

    /**
     * Get the list of possible dataset names available for this {@code Datastore} or an empty List if none.
     * This method can cope with a tree of choices to access to the datasets as long as they can be
     * represented with a string.
     * for example in Cassadra a call to
     * <code>getPossibleDatasetNames(container, null)</code>
     * will return the list of all keyspaces, then a call to
     * <code>getPossibleDatasetNames(container, "keyspaceOne")</code>
     * will return all the table associated tables related to *keyspaceOne*.
     * <p>
     * This uses the {@link ComponentProperties} previously specified to make any necessary connection.
     * It is not intended to update any associated
     * {@code ComponentProperties} object.
     * 
     * @param container give information of the container running this class.
     * @param datasetPath, helps locate the dataset in a tree architecture, a "/" will be use to separate each
     *            path. May be null if you want to retrieve root dataset path.
     */
    List<NamedThing> getPossibleDatasetNames(RuntimeContainer container, String datasetPath) throws IOException;

    /**
     * perform a series of health checks like cheking the connection is possible or the status of each clusters.
     * This method will be called in the same process where the runtime will actually be executed.
     */
    Iterable<ValidationResult> doHealthChecks(RuntimeContainer container);
}
