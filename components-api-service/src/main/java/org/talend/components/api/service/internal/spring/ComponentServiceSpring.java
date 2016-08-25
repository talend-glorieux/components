// ============================================================================
//
// Copyright (C) 2006-2015 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.api.service.internal.spring;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.view.RedirectView;
import org.talend.components.api.component.ComponentDefinition;
import org.talend.components.api.component.ComponentImageType;
import org.talend.components.api.component.Connector;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.api.service.ComponentService;
import org.talend.components.api.service.internal.ComponentRegistry;
import org.talend.components.api.service.internal.ComponentServiceImpl;
import org.talend.components.api.wizard.ComponentWizard;
import org.talend.components.api.wizard.ComponentWizardDefinition;
import org.talend.components.api.wizard.WizardImageType;
import org.talend.daikon.exception.error.CommonErrorCodes;
import org.talend.daikon.properties.Properties;
import org.talend.daikon.properties.service.Repository;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;

/**
 * This is a spring only class that is instantiated by the spring framework. It delegates all its calls to the
 * ComponentServiceImpl delegate create in it's constructor. This delegate uses a Component registry implementation
 * specific to spring.
 */

@RestController
@Api(value = "components", basePath = ComponentServiceSpring.BASE_PATH, description = "Component services")
@Service
public class ComponentServiceSpring implements ComponentService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComponentServiceSpring.class);

    public static final String BASE_PATH = "/components"; //$NON-NLS-1$

    private ComponentService componentServiceDelegate;

    @Autowired
    public ComponentServiceSpring(final ApplicationContext context) {
        this.componentServiceDelegate = new ComponentServiceImpl(new ComponentRegistry() {

            @Override
            public Map<String, ComponentDefinition> getComponents() {
                Map<String, ComponentDefinition> compDefs = context.getBeansOfType(ComponentDefinition.class);
                return compDefs;
            }

            @Override
            public Map<String, ComponentWizardDefinition> getComponentWizards() {
                Map<String, ComponentWizardDefinition> wizardDefs = context.getBeansOfType(ComponentWizardDefinition.class);
                return wizardDefs;
            }

        });
    }

    @Override
    @RequestMapping(value = BASE_PATH
            + "/properties/{name}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody ComponentProperties getComponentProperties(
            @PathVariable(value = "name") @ApiParam(name = "name", value = "Name of the component") String name) {
        return componentServiceDelegate.getComponentProperties(name);
    }

    @Override
    @RequestMapping(value = BASE_PATH
            + "/definition/{name}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody ComponentDefinition getComponentDefinition(
            @PathVariable(value = "name") @ApiParam(name = "name", value = "Name of the component") String name) {
        return componentServiceDelegate.getComponentDefinition(name);
    }

    @Override
    @RequestMapping(value = BASE_PATH
            + "/dependencies/{name}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody Set<String> getMavenUriDependencies(
            @PathVariable(value = "name") @ApiParam(name = "name", value = "Name of the component") String name) {
        return componentServiceDelegate.getMavenUriDependencies(name);
    }

    @Override
    @RequestMapping(value = BASE_PATH
            + "/wizard/{name}/{repositoryLocation}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public ComponentWizard getComponentWizard(
            @PathVariable(value = "name") @ApiParam(name = "name", value = "Name of the component") String name,
            @PathVariable(value = "repositoryLocation") @ApiParam(name = "repositoryLocation", value = "Repository location") String repositoryLocation) {
        return componentServiceDelegate.getComponentWizard(name, repositoryLocation);
    }

    @Override
    @RequestMapping(value = BASE_PATH
            + "/wizardForProperties/{repositoryLocation}", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    public List<ComponentWizard> getComponentWizardsForProperties(
            @ApiParam(name = "properties", value = "Component properties") @RequestBody ComponentProperties properties,
            @PathVariable(value = "repositoryLocation") @ApiParam(name = "repositoryLocation", value = "Repository location") String repositoryLocation) {
        return componentServiceDelegate.getComponentWizardsForProperties(properties, repositoryLocation);
    }

    @Override
    @RequestMapping(value = BASE_PATH
            + "/possibleComponents", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    public List<ComponentDefinition> getPossibleComponents(
            @ApiParam(name = "properties", value = "Component properties") @RequestBody ComponentProperties... properties)
                    throws Throwable {
        return componentServiceDelegate.getPossibleComponents(properties);
    }

    @Override
    @RequestMapping(value = BASE_PATH
            + "/makeFormCancelable", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    public Properties makeFormCancelable(
            @ApiParam(name = "properties", value = "Properties related to the form") @RequestBody Properties properties,
            @ApiParam(name = "formName", value = "Name of the form") String formName) {
        return componentServiceDelegate.makeFormCancelable(properties, formName);
    }

    @Override
    @RequestMapping(value = BASE_PATH
            + "/commitFormValues", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    public Properties cancelFormValues(
            @ApiParam(name = "properties", value = "Properties related to the form.") @RequestBody Properties properties,
            @ApiParam(name = "formName", value = "Name of the form") String formName) {
        return componentServiceDelegate.cancelFormValues(properties, formName);
    }

    @Override
    @RequestMapping(value = BASE_PATH
            + "/properties/{propName}/validate", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody Properties validateProperty(
            @PathVariable(value = "propName") @ApiParam(name = "propName", value = "Name of property") String propName,
            @ApiParam(name = "properties", value = "Properties holding the property to validate") @RequestBody Properties properties)
                    throws Throwable {
        componentServiceDelegate.validateProperty(propName, properties);
        return properties;
    }

    @Override
    @RequestMapping(value = BASE_PATH
            + "/properties/{propName}/beforeActivate", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody Properties beforePropertyActivate(
            @PathVariable(value = "propName") @ApiParam(name = "propName", value = "Name of property") String propName,
            @ApiParam(name = "properties", value = "Properties holding the property to activate") @RequestBody Properties properties)
                    throws Throwable {
        componentServiceDelegate.beforePropertyActivate(propName, properties);
        return properties;
    }

    @Override
    @RequestMapping(value = BASE_PATH
            + "/properties/{propName}/beforeRender", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody Properties beforePropertyPresent(
            @PathVariable(value = "propName") @ApiParam(name = "propName", value = "Name of property") String propName,
            @ApiParam(name = "properties", value = "Properties holding the property that is going to be presented") @RequestBody Properties properties)
                    throws Throwable {
        componentServiceDelegate.beforePropertyPresent(propName, properties);
        return properties;
    }

    @Override
    @RequestMapping(value = BASE_PATH
            + "/properties/{propName}/after", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody Properties afterProperty(
            @PathVariable(value = "propName") @ApiParam(name = "propName", value = "Name of property") String propName,
            @ApiParam(name = "properties", value = "Properties holding the value that just has been set") @RequestBody Properties properties)
                    throws Throwable {
        componentServiceDelegate.afterProperty(propName, properties);
        return properties;
    }

    @Override
    @RequestMapping(value = BASE_PATH
            + "/properties/beforeFormPresent/{formName}", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody Properties beforeFormPresent(
            @PathVariable(value = "formName") @ApiParam(name = "formName", value = "Name of form") String formName,
            @ApiParam(name = "properties", value = "Properties holding the form to be presented") @RequestBody Properties properties)
                    throws Throwable {
        componentServiceDelegate.beforeFormPresent(formName, properties);
        return properties;
    }

    @Override
    @RequestMapping(value = BASE_PATH
            + "/properties/afterFormNext/{formName}", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody Properties afterFormNext(
            @PathVariable(value = "formName") @ApiParam(name = "formName", value = "Name of form") String formName,
            @ApiParam(name = "properties", value = "Properties related to the current wizard form") @RequestBody Properties properties)
                    throws Throwable {
        componentServiceDelegate.afterFormNext(formName, properties);
        return properties;
    }

    @Override
    @RequestMapping(value = BASE_PATH
            + "/properties/afterFormBack/{formName}", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody Properties afterFormBack(
            @PathVariable(value = "formName") @ApiParam(name = "formName", value = "Name of form") String formName,
            @ApiParam(name = "properties", value = "Properties related to the current form") @RequestBody Properties properties)
                    throws Throwable {
        componentServiceDelegate.afterFormBack(formName, properties);
        return properties;
    }

    @Override
    @RequestMapping(value = BASE_PATH
            + "/properties/afterFormFinish/{formName}", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody Properties afterFormFinish(
            @PathVariable(value = "formName") @ApiParam(name = "formName", value = "Name of form") String formName,
            @ApiParam(name = "properties", value = "Properties holding the current form to be closed.") @RequestBody Properties properties)
                    throws Throwable {
        componentServiceDelegate.afterFormFinish(formName, properties);
        return properties;
    }

    @Override
    @RequestMapping(value = BASE_PATH + "/names", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody Set<String> getAllComponentNames() {
        return componentServiceDelegate.getAllComponentNames();
    }

    @Override
    @RequestMapping(value = BASE_PATH + "/definitions", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody Set<ComponentDefinition> getAllComponents() {
        return componentServiceDelegate.getAllComponents();
    }

    @RequestMapping(value = BASE_PATH
            + "/wizards/definitions", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @Override
    public Set<ComponentWizardDefinition> getTopLevelComponentWizards() {
        return componentServiceDelegate.getTopLevelComponentWizards();
    }

    @Override
    // this cannot be used as is as a rest api so see getWizardImageRest.
    public InputStream getWizardPngImage(String wizardName, WizardImageType imageType) {
        return componentServiceDelegate.getWizardPngImage(wizardName, imageType);
    }

    @RequestMapping(value = BASE_PATH
            + "/wizards/{name}/icon/{type}", method = RequestMethod.GET, produces = MediaType.IMAGE_PNG_VALUE)
    @ApiOperation(value = "Return the icon related to the wizard", notes = "return the png image related to the wizard parameter.")
    public void getWizardImageRest(@PathVariable(value = "name") @ApiParam(name = "name", value = "Name of wizard") String name,
            @PathVariable(value = "type") @ApiParam(name = "type", value = "Type of the icon requested") WizardImageType type,
            final HttpServletResponse response) {
        InputStream wizardPngImageStream = getWizardPngImage(name, type);
        sendStreamBack(response, wizardPngImageStream);
    }

    private void sendStreamBack(final HttpServletResponse response, InputStream inputStream) {
        try {
            if (inputStream != null) {
                try {
                    IOUtils.copy(inputStream, response.getOutputStream());
                } catch (IOException e) {
                    throw new ComponentException(CommonErrorCodes.UNEXPECTED_EXCEPTION, e);
                } finally {
                    inputStream.close();
                }
            } else {// could not get icon so respond a resource_not_found : 404
                response.sendError(HttpServletResponse.SC_NOT_FOUND);
            }
        } catch (IOException e) {// is sendError fails or inputstream fails when closing
            LOGGER.error("sendError failed or inputstream failed when closing.", e); //$NON-NLS-1$
            throw new ComponentException(CommonErrorCodes.UNEXPECTED_EXCEPTION, e);
        }
    }

    @Override
    // this cannot be used as is as a rest api so see getWizardPngIconRest.
    public InputStream getComponentPngImage(String componentName, ComponentImageType imageType) {
        return componentServiceDelegate.getComponentPngImage(componentName, imageType);
    }

    @Override
    public void setRepository(Repository repository) {
        componentServiceDelegate.setRepository(repository);
    }

    @RequestMapping(value = BASE_PATH + "/icon/{name}", method = RequestMethod.GET, produces = MediaType.IMAGE_PNG_VALUE)
    @ApiOperation(value = "Return the icon related to the Component", notes = "return the png image related to the Component name parameter.")
    public void getComponentsImageRest(
            @PathVariable(value = "name") @ApiParam(name = "name", value = "Name of Component") String name,
            @PathVariable(value = "type") @ApiParam(name = "type", value = "Type of the icon requested") ComponentImageType type,
            final HttpServletResponse response) {
        InputStream componentPngImageStream = getComponentPngImage(name, type);
        sendStreamBack(response, componentPngImageStream);
    }

    @RequestMapping(value = BASE_PATH + "/components/{name}/{type}", method = RequestMethod.POST)
    @ApiOperation(value = "Return where to interact with the runtime", notes = "return where to interact with the runtime.")
    public RedirectView setupComponentRuntimeFromRestAPI(
            @ApiParam(name = "name", value = "Name of the component") @PathVariable(value = "name") String name,
            @ApiParam(name = "type", value = "Target component type, e.g. source, sink...") @PathVariable(value = "type") String type,
            @ApiParam(name = "properties", value = "Properties to setup the component.") @RequestBody ComponentProperties properties) {

        LOGGER.debug("setting up runtime for component {} -> {} with {} properties", name, type, properties.getProperties().size());

        String id = setupComponentRuntime(name, type, properties);
        LOGGER.info("runtime setup for {} / {} --> {}", name, type, id);

        return new RedirectView("/components/"+ name + '/'+ type + '/' + id);
    }


    @Override
    public String setupComponentRuntime(String name, String type, ComponentProperties properties) {
        return componentServiceDelegate.setupComponentRuntime(name, type, properties);
    }

    @RequestMapping(value = BASE_PATH + "/components/{name}/{type}/{id}", method = RequestMethod.GET)
    @ApiOperation(value = "Execute the 'source' runtime and return its content within the http response body", notes = "return the runtime content.")
    public void readRuntimeInputFromRest(
            @ApiParam(name = "name", value = "Name of the component") @PathVariable(value = "name") String name,
            @ApiParam(name = "type", value = "Target component type, e.g. source, sink...") @PathVariable(value = "type") String type,
            @ApiParam(name = "id", value = "The runtime id") @PathVariable(value = "id") String id,
            OutputStream response) {

        LOGGER.debug("execute runtime {} - {} - {}", name, type, id);
        readRuntimeInput(name, type, id, response);
    }


    @Override
    public void readRuntimeInput(String name, String type, String id, OutputStream output) {
        componentServiceDelegate.readRuntimeInput(name, type, id, output);
    }


    // FIXME - make this work for web
    @Override
    public String storeProperties(Properties properties, String name, String repositoryLocation, String schemaPropertyName) {
        return componentServiceDelegate.storeProperties(properties, name, repositoryLocation, schemaPropertyName);
    }

    @Override
    public Schema getSchema(ComponentProperties componentProperties, Connector connector, boolean isOuput) {
        return componentServiceDelegate.getSchema(componentProperties, connector, isOuput);
    }

    @Override
    public Set<? extends Connector> getAvailableConnectors(ComponentProperties componentProperties,
            Set<? extends Connector> connectedConnetor, boolean isOuput) {
        return componentServiceDelegate.getAvailableConnectors(componentProperties, connectedConnetor, isOuput);
    }

    @Override
    public void setSchema(ComponentProperties componentProperties, Connector connector, Schema schema, boolean isOuput) {
        componentServiceDelegate.setSchema(componentProperties, connector, schema, isOuput);
    }

    @Override
    public boolean setNestedPropertiesValues(ComponentProperties targetProperties, Properties nestedValues) {
        return componentServiceDelegate.setNestedPropertiesValues(targetProperties, nestedValues);
    }

}
