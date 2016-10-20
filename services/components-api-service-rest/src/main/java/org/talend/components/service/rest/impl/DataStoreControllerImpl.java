package org.talend.components.service.rest.impl;

import static java.util.stream.StreamSupport.stream;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Optional;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.talend.components.api.service.ComponentService;
import org.talend.components.common.datastore.DatastoreDefinition;
import org.talend.components.service.rest.DataStoreController;
import org.talend.daikon.annotation.ServiceImplementation;

/**
 * Rest controller in charge of datastores.
 */
@ServiceImplementation
public class DataStoreControllerImpl implements DataStoreController {

    /** This class' logger. */
    private static final Logger LOGGER = getLogger(DataStoreControllerImpl.class);

    @Autowired
    private ComponentService componentServiceDelegate;

    @Override
    public Iterable<DatastoreDefinition> listDataStoreDefinitions() {
        LOGGER.debug("listing datastore definitions");
        return componentServiceDelegate.getDefinitionsByType(DatastoreDefinition.class);
    }

    @Override
    public DatastoreDefinition getDatastoreDefinition(String datastoreName) {

        final Iterable<DatastoreDefinition> iterable = componentServiceDelegate.getDefinitionsByType(DatastoreDefinition.class);

        final Optional<DatastoreDefinition> first = stream(iterable.spliterator(), true) //
                .filter(def -> datastoreName.equals(def.getName())) //
                .findFirst();

        final DatastoreDefinition result;
        if (first.isPresent()) {
            result = first.get();
        } else {
            result = null;
        }

        LOGGER.debug("found datastore definition {} for {}", result, datastoreName);

        return result;
    }

    @Override
    public void validateDatastoreDefinition(String datastoreName, DatastoreDefinition definition) {

        // TODO à faire !
        LOGGER.debug("validate {}", definition);

    }

}
