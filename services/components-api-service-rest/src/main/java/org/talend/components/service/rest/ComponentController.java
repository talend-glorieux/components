package org.talend.components.service.rest;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.MediaType.IMAGE_PNG_VALUE;
import static org.springframework.web.bind.annotation.RequestMethod.GET;

import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import org.springframework.web.bind.annotation.*;
import org.talend.components.api.component.ComponentDefinition;
import org.talend.components.api.component.ComponentImageType;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.api.wizard.ComponentWizard;
import org.talend.components.api.wizard.WizardImageType;
import org.talend.daikon.annotation.Service;
import org.talend.daikon.properties.Properties;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.service.Repository;

/**
 *
 */
@Service(name = "ComponentController")
public interface ComponentController {

    String BASE_PATH = "/components";


    /**
     * Get the list of all the component names that are registered
     *
     * @return the set of component names, never null.
     *
     * @returnWrapped org.talend.components.api.properties.ComponentProperties
     */
    @RequestMapping(value = BASE_PATH + "/properties/{name}", method = GET, produces = APPLICATION_JSON_VALUE)
    @ResponseBody
    ComponentProperties getComponentProperties(@PathVariable(value = "name") String name);


    /**
     * Get the list of all the components {@link ComponentDefinition} that are registered
     *
     * @return the set of component definitions, never null.
     *
     * @returnWrapped org.talend.components.api.component.ComponentDefinition
     */
    @RequestMapping(value = BASE_PATH + "/definition/{name}", method = GET, produces = APPLICATION_JSON_VALUE)
    @ResponseBody
    ComponentDefinition getComponentDefinition(@PathVariable(value = "name") String name);


    /**
     * Creates a new instance of a {@link ComponentWizard} for the specified wizard name.
     *
     * Wizard names are globally unique. Non-top-level wizards should be named using the name of the top-level wizard.
     * For example, the Salesforce wizard would be called "salesforce", and a wizard dealing with only the Salesforce
     * modules would be called "salesforce.modules" in order to make sure the name is unique.
     *
     * @param name the name of the wizard
     * @param repositoryLocation an arbitrary repositoryLocation string to optionally be used in the wizard processing. This is
     * given to an implementation of the {@link Repository} object when the {@link ComponentProperties} are stored.
     * @return a {@code ComponentWizard} object.
     *
     * @returnWrapped org.talend.components.api.wizard.ComponentWizard
     */
    @RequestMapping(value = BASE_PATH + "/wizard/{name}/{repositoryLocation}", method = GET, produces = APPLICATION_JSON_VALUE)
    ComponentWizard getComponentWizard(@PathVariable(value = "name") String name,
            @PathVariable(value = "repositoryLocation") String repositoryLocation);


    /**
     * Creates {@link ComponentWizard}(s) that are populated by the given properties.
     *
     * This is used when you already have the object from a previous execution of the wizard and you wish to
     * show wizards applicable to the those properties.
     *
     * @param properties a  object previously created
     * @param repositoryLocation the repository location of where the were stored.
     * @return a {@link List} of {@code ComponentWizard} object(s)
     *
     * @returnWrapped java.util.List<org.talend.components.api.wizard.ComponentWizard>
     */
    @RequestMapping(value = BASE_PATH + "/wizardForProperties/{repositoryLocation}", method = RequestMethod.POST, produces = APPLICATION_JSON_VALUE)
    List<ComponentWizard> getComponentWizardsForProperties( //
            @RequestBody ComponentProperties properties, //
            @PathVariable(value = "repositoryLocation") String repositoryLocation);

    /**
     * Return the {@link ComponentDefinition} objects for any component(s) that can be constructed from the given
     * {@link ComponentProperties} object.
     *
     * @param properties the {@link ComponentProperties} object to look for.
     * @return the list of compatible {@link ComponentDefinition} objects.
     *
     * @returnWrapped java.util.List<org.talend.components.api.component.ComponentDefinition>
     */
    @RequestMapping(value = BASE_PATH + "/possibleComponents", method = RequestMethod.POST, produces = APPLICATION_JSON_VALUE)
    List<ComponentDefinition> getPossibleComponents(@RequestBody ComponentProperties... properties) throws Throwable;


    /**
     * Makes the specified {@link Form} object cancelable, which means that modifications to the values can be canceled.
     *
     * This is intended for local use only. When using this with the REST service, the values can simply be reset in the JSON
     * version of the {@link Form} object, so the cancel operation can be implemented entirely by the client.
     * @param properties the {@link Properties} object associated with the {@code Form}.
     * @param formName the name of the form
     * @return the {@link Properties} object specified as modified by this service.
     *
     * @returnWrapped org.talend.daikon.properties.Properties
     */
    @RequestMapping(value = BASE_PATH + "/makeFormCancelable", method = RequestMethod.POST, produces = APPLICATION_JSON_VALUE)
    Properties makeFormCancelable(@RequestBody Properties properties, String formName);


    /**
     * Cancels the changes to the values in the specified {@link Form} object after the it was made cancelable.
     *
     * This is intended for local use only. When using this with the REST service, the values can simply be reset in the JSON
     * version of the {@link Form} object, so the cancel operation can be implemented entirely by the client.
     * @param properties the {@link Properties} object associated with the {@code Form}.
     * @param formName the name of the form
     * @return the {@link Properties} object specified as modified by this service.
     *
     * @returnWrapped org.talend.daikon.properties.Properties
     */
    @RequestMapping(value = BASE_PATH + "/commitFormValues", method = RequestMethod.POST, produces = APPLICATION_JSON_VALUE)
    Properties cancelFormValues(@RequestBody Properties properties, String formName);


    /**
     * @see {@link Properties} for a description of the meaning of this method.
     * @return the {@link Properties} object specified as modified by this service.
     *
     * @returnWrapped org.talend.daikon.properties.Properties
     */
    @RequestMapping(value = BASE_PATH
            + "/properties/{propName}/validate", method = RequestMethod.POST, produces = APPLICATION_JSON_VALUE)
    @ResponseBody
    Properties validateProperty(@PathVariable(value = "propName") String propName, @RequestBody Properties properties)
            throws Throwable;


    /**
     * @see {@link Properties} for a description of the meaning of this method.
     * @return the {@link Properties} object specified as modified by this service.
     *
     * @returnWrapped org.talend.daikon.properties.Properties
     */
    @RequestMapping(value = BASE_PATH
            + "/properties/{propName}/beforeActivate", method = RequestMethod.POST, produces = APPLICATION_JSON_VALUE)
    @ResponseBody
    Properties beforePropertyActivate(@PathVariable(value = "propName") String propName, @RequestBody Properties properties)
            throws Throwable;


    /**
     * @see {@link Properties} for a description of the meaning of this method.
     * @return the {@link Properties} object specified as modified by this service.
     *
     * @returnWrapped org.talend.daikon.properties.Properties
     */
    @RequestMapping(value = BASE_PATH
            + "/properties/{propName}/beforeRender", method = RequestMethod.POST, produces = APPLICATION_JSON_VALUE)
    @ResponseBody
    Properties beforePropertyPresent(@PathVariable(value = "propName") String propName, @RequestBody Properties properties)
            throws Throwable;


    /**
     * @see {@link Properties} for a description of the meaning of this method.
     * @return the {@link Properties} object specified as modified by this service.
     *
     * @returnWrapped org.talend.daikon.properties.Properties
     */
    @RequestMapping(value = BASE_PATH
            + "/properties/{propName}/after", method = RequestMethod.POST, produces = APPLICATION_JSON_VALUE)
    @ResponseBody
    Properties afterProperty(@PathVariable(value = "propName") String propName, @RequestBody Properties properties)
            throws Throwable;


    /**
     * @see {@link Properties} for a description of the meaning of this method.
     * @return the {@link Properties} object specified as modified by this service.
     *
     * @returnWrapped org.talend.daikon.properties.Properties
     */
    @RequestMapping(value = BASE_PATH
            + "/properties/beforeFormPresent/{formName}", method = RequestMethod.POST, produces = APPLICATION_JSON_VALUE)
    @ResponseBody
    Properties beforeFormPresent(@PathVariable(value = "formName") String formName, @RequestBody Properties properties)
            throws Throwable;


    /**
     * @see {@link Properties} for a description of the meaning of this method.
     * @return the {@link Properties} object specified as modified by this service.
     *
     * @returnWrapped org.talend.daikon.properties.Properties
     */
    @RequestMapping(value = BASE_PATH
            + "/properties/afterFormNext/{formName}", method = RequestMethod.POST, produces = APPLICATION_JSON_VALUE)
    @ResponseBody
    Properties afterFormNext(@PathVariable(value = "formName") String formName, @RequestBody Properties properties)
            throws Throwable;



    /**
     * @see {@link Properties} for a description of the meaning of this method.
     * @return the {@link Properties} object specified as modified by this service.
     *
     * @returnWrapped org.talend.daikon.properties.Properties
     */
    @RequestMapping(value = BASE_PATH
            + "/properties/afterFormBack/{formName}", method = RequestMethod.POST, produces = APPLICATION_JSON_VALUE)
    @ResponseBody
    Properties afterFormBack(@PathVariable(value = "formName") String formName, @RequestBody Properties properties)
            throws Throwable;


    /**
     * @see {@link Properties} for a description of the meaning of this method.
     * @return the {@link Properties} object specified as modified by this service.
     *
     * @returnWrapped org.talend.daikon.properties.Properties
     */
    @RequestMapping(value = BASE_PATH
            + "/properties/afterFormFinish/{formName}", method = RequestMethod.POST, produces = APPLICATION_JSON_VALUE)
    @ResponseBody
    Properties afterFormFinish(@PathVariable(value = "formName") String formName, @RequestBody Properties properties)
            throws Throwable;


    /**
     * Get the list of all the component names that are registered
     *
     * @return the set of component names, never null
     *
     * @returnWrapped java.util.Set<java.lang.String>
     */
    @RequestMapping(value = BASE_PATH + "/names", method = GET, produces = APPLICATION_JSON_VALUE)
    @ResponseBody
    Set<String> getAllComponentNames();


    /**
     * Get the list of all the components {@link ComponentDefinition} that are registered
     *
     * @return the set of component definitions, never null.
     *
     * @returnWrapped java.util.Set<org.talend.components.api.component.ComponentDefinition>
     */
    @RequestMapping(value = BASE_PATH + "/definitions", method = GET, produces = APPLICATION_JSON_VALUE)
    @ResponseBody
    Set<ComponentDefinition> getAllComponents();


    /**
     * Return the png image related to the given wizard
     *
     * @param name, name of the wizard to get the image for
     * @param type, the type of image requested
     * @return the png image stream or null if none was provided or could not be found
     * @exception ComponentException thrown if the componentName is not registered in the service
     *
     * TODO change the method signature not to deal with HttpServletResponse
     */
    @RequestMapping(value = BASE_PATH + "/wizards/{name}/icon/{type}", method = GET, produces = IMAGE_PNG_VALUE)
    void getWizardImageRest(@PathVariable(value = "name") String name, @PathVariable(value = "type") WizardImageType type,
            HttpServletResponse response);


    /**
     * Return the png image related to the given component
     *
     * @param name, name of the comonent to get the image for
     * @param type, the type of image requested
     * @return the png image stream or null if none was provided or an error occurred
     * @exception ComponentException thrown if the componentName is not registered in the service
     */
    @RequestMapping(value = BASE_PATH + "/icon/{name}", method = GET, produces = IMAGE_PNG_VALUE)
    void getComponentsImageRest(@PathVariable(value = "name") String name, @PathVariable(value = "type") ComponentImageType type,
            HttpServletResponse response);

}
