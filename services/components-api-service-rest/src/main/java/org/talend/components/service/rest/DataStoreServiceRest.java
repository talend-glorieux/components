package org.talend.components.service.rest;

import static java.util.stream.StreamSupport.stream;
import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;
import static org.talend.components.service.rest.configuration.ComponentsSetup.BASE_COMPONENT_SERVICE_ID;

import java.util.Optional;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.talend.components.api.service.ComponentService;
import org.talend.components.common.datastore.DatastoreDefinition;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;

/**
 *
 */
@RestController
@Api(value = "datastores", description = "Datastores services")
public class DataStoreServiceRest {

    /** This class' logger. */
    private static final Logger LOGGER = getLogger(DataStoreServiceRest.class);

    @Autowired
    @Qualifier(BASE_COMPONENT_SERVICE_ID)
    private ComponentService componentServiceDelegate;


    @RequestMapping(value = "/definitions/datastores", method = GET, produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Get the known datastores definitions.", notes = "List all known datastore definitions.")
    public Iterable<DatastoreDefinition> listDataStoreDefinitions() {
        LOGGER.debug("listing datastore definitions");
        return componentServiceDelegate.getDefinitionsByType(DatastoreDefinition.class);
    }


    @RequestMapping(value = "/definitions/datastores/{datastoreName}", method = GET, produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Get the current datastores definition.", notes = "Return the current datastore definitions.")
    public DatastoreDefinition getDatastoreDefinition(@PathVariable(value = "datastoreName") @ApiParam(name = "datastoreName", value = "Name of the datastore") String datastoreName) {


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


    @RequestMapping(value = "/definitions/datastores/{datastoreName}/validates", method = POST, produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Get the current datastores definition.", notes = "Return the current datastore definitions.")
    public void validateDatastoreDefinition(
            @PathVariable(value = "datastoreName") @ApiParam(name = "datastoreName", value = "Name of the datastore") String datastoreName,
            @RequestBody DatastoreDefinition definition) {

        // TODO à faire !
        LOGGER.debug("validate {}", definition);

    }



}
