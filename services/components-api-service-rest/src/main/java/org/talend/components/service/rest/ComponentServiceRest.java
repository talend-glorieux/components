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
package org.talend.components.service.rest;

import static org.talend.components.service.rest.configuration.ComponentsSetup.BASE_COMPONENT_SERVICE_ID;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;
import org.talend.components.api.RuntimableDefinition;
import org.talend.components.api.component.ComponentDefinition;
import org.talend.components.api.component.ComponentImageType;
import org.talend.components.api.component.Connector;
import org.talend.components.api.component.ConnectorTopology;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.api.service.ComponentService;
import org.talend.components.api.service.common.ComponentServiceImpl;
import org.talend.components.api.wizard.ComponentWizard;
import org.talend.components.api.wizard.ComponentWizardDefinition;
import org.talend.components.api.wizard.WizardImageType;
import org.talend.daikon.exception.error.CommonErrorCodes;
import org.talend.daikon.properties.Properties;
import org.talend.daikon.properties.service.Repository;
import org.talend.daikon.runtime.RuntimeInfo;

import com.google.common.collect.Lists;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;

/**
 * This is a spring only class that is instantiated by the spring framework. It delegates all its calls to the
 * ComponentServiceImpl delegate create in it's constructor. This delegate uses a Component registry implementation
 * specific to spring.
 */

@RestController
@Api(value = "components", basePath = ComponentServiceRest.BASE_PATH, description = "Component services")
@Service
public class ComponentServiceRest implements ComponentService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComponentServiceImpl.class);

    public static final String BASE_PATH = "/components"; //$NON-NLS-1$

    @Autowired
    @Qualifier(BASE_COMPONENT_SERVICE_ID)
    private ComponentService componentServiceDelegate;


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

    @Override
    public <T extends RuntimableDefinition<?, ?>> Iterable<T> getDefinitionsByType(Class<T> cls) {
        // Needs to be copied into a new list to be serialized remotely.
        return Lists.newArrayList(componentServiceDelegate.getDefinitionsByType(cls));
    }

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

    @Override
    public RuntimeInfo getRuntimeInfo(String componentName, Properties properties, ConnectorTopology componentType) {
        return componentServiceDelegate.getRuntimeInfo(componentName, properties, componentType);
    }

}
