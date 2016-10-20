package org.talend.components.service.rest;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.talend.components.common.datastore.DatastoreDefinition;
import org.talend.daikon.annotation.Service;

/**
 * Interface for the data store controller.
 */
@Service(name = "DataStoreController")
public interface DataStoreController {

    /**
     * Return all known DataStore definitions.
     * @return all known DataStore definitions.
     *
     * @returnWrapped java.lang.Iterable<org.talend.components.common.datastore.DatastoreDefinition>
     */
    @RequestMapping(value = "/definitions/datastores", method = GET, produces = APPLICATION_JSON_VALUE)
    Iterable<DatastoreDefinition> listDataStoreDefinitions();

    /**
     * Return the wanted DataStore definition.
     * @param datastoreName the name of the wanted datastore.
     * @return the wanted DataStore definition.
     *
     * @returnWrapped org.talend.components.common.datastore.DatastoreDefinition
     */
    @RequestMapping(value = "/definitions/datastores/{datastoreName}", method = GET, produces = APPLICATION_JSON_VALUE)
    DatastoreDefinition getDatastoreDefinition(@PathVariable(value = "datastoreName") String datastoreName);

    /**
     * Validates the given datastore definitions.
     *
     * @param datastoreName the name of the datastore to validate.
     * @param definition the datastore properties to validate.
     *
     * @HTTP 204 If the given definition is valid.
     * @HTTP 400 If the given definition is not valid.
     */
    @RequestMapping(value = "/definitions/datastores/{datastoreName}/validates", method = POST, produces = APPLICATION_JSON_VALUE)
    void validateDatastoreDefinition(@PathVariable(value = "datastoreName") String datastoreName, @RequestBody DatastoreDefinition definition);

}
