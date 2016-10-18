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
package org.talend.components.api.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

/**
 * Provides a context for building into the current Beam {@link Pipeline} containing the job.
 *
 * This context includes all of the changes that have been added to the pipeline by components up to this point, and any
 * component building into the context is responsible for adding its results back into this context.
 */
public interface BeamJobContext {

    PCollection getPCollectionByLinkName(String linkName);

    void putPCollectionByLinkName(String linkName, PCollection pcollection);

    String getLinkNameByPortName(String portName);

    Pipeline getPipeline();

}
