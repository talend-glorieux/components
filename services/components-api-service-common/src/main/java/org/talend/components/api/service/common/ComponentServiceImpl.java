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
package org.talend.components.api.service.common;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.RuntimableDefinition;
import org.talend.components.api.component.ComponentDefinition;
import org.talend.components.api.component.ComponentImageType;
import org.talend.components.api.component.Connector;
import org.talend.components.api.component.ConnectorTopology;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.api.exception.error.ComponentsApiErrorCode;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.api.service.ComponentService;
import org.talend.components.api.wizard.ComponentWizard;
import org.talend.components.api.wizard.ComponentWizardDefinition;
import org.talend.components.api.wizard.WizardImageType;
import org.talend.daikon.NamedThing;
import org.talend.daikon.exception.ExceptionContext;
import org.talend.daikon.properties.Properties;
import org.talend.daikon.properties.service.PropertiesServiceImpl;
import org.talend.daikon.runtime.RuntimeInfo;

/**
 * Main Component Service implementation that is not related to any framework (neither OSGI, nor Spring) it uses a
 * ComponentRegistry implementation that will be provided by framework specific Service classes
 */
public class ComponentServiceImpl extends PropertiesServiceImpl implements ComponentService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComponentServiceImpl.class);

    private ComponentRegistry componentRegistry;

    public ComponentServiceImpl(ComponentRegistry componentRegistry) {
        this.componentRegistry = componentRegistry;
    }

    @Override
    public Set<String> getAllComponentNames() {
        Set<String> names = new HashSet<>();
        for (ComponentDefinition def : getDefinitionsByType(ComponentDefinition.class)) {
            names.add(def.getName());
        }
        return names;
    }

    @Override
    public Set<ComponentDefinition> getAllComponents() {
        // If we ever add a guava dependency: return Sets.newHashSet(getDefinitionsByType...)
        Set<ComponentDefinition> defs = new HashSet<>();
        for (ComponentDefinition def : componentRegistry.getDefinitionsByType(ComponentDefinition.class)) {
            defs.add(def);
        }
        return defs;
    }

    @Override
    public <T extends RuntimableDefinition<?, ?>> Iterable<T> getDefinitionsByType(Class<T> cls) {
        return componentRegistry.getDefinitionsByType(cls);
    }

    @Override
    public Set<ComponentWizardDefinition> getTopLevelComponentWizards() {
        Set<ComponentWizardDefinition> defs = new HashSet<>();
        for (ComponentWizardDefinition def : componentRegistry.getComponentWizards().values()) {
            if (def.isTopLevel()) {
                defs.add(def);
            }
        }
        return defs;
    }

    @Override
    public ComponentProperties getComponentProperties(String name) {
        ComponentDefinition compDef = getComponentDefinition(name);
        ComponentProperties properties = compDef.createProperties();
        return properties;
    }

    @Override
    public ComponentDefinition getComponentDefinition(String name) {
        for (ComponentDefinition def : componentRegistry.getDefinitionsByType(ComponentDefinition.class)) {
            if (name.equals(def.getName())) {
                return def;
            }
        }
        // The component was not found.
        throw new ComponentException(ComponentsApiErrorCode.WRONG_COMPONENT_NAME, ExceptionContext.build().put("name", name)); //$NON-NLS-1$
    }

    @Override
    public ComponentWizard getComponentWizard(String name, String location) {
        ComponentWizardDefinition wizardDefinition = componentRegistry.getComponentWizards().get(name);
        if (wizardDefinition == null) {
            throw new ComponentException(ComponentsApiErrorCode.WRONG_WIZARD_NAME, ExceptionContext.build().put("name", name)); //$NON-NLS-1$
        }
        ComponentWizard wizard = wizardDefinition.createWizard(location);
        return wizard;
    }

    @Override
    public List<ComponentWizard> getComponentWizardsForProperties(ComponentProperties properties, String location) {
        List<ComponentWizard> wizards = new ArrayList<>();
        for (ComponentWizardDefinition wizardDefinition : componentRegistry.getComponentWizards().values()) {
            if (wizardDefinition.supportsProperties(properties.getClass())) {
                ComponentWizard wizard = wizardDefinition.createWizard(properties, location);
                wizards.add(wizard);
            }
        }
        return wizards;
    }

    @Override
    public List<ComponentDefinition> getPossibleComponents(ComponentProperties... properties) {
        List<ComponentDefinition> returnList = new ArrayList<>();
        for (ComponentDefinition cd : componentRegistry.getDefinitionsByType(ComponentDefinition.class)) {
            if (cd.supportsProperties(properties)) {
                returnList.add(cd);
            }
        }
        return returnList;
    }

    @Override
    public boolean setNestedPropertiesValues(ComponentProperties targetProperties, Properties nestedValues) {
        return targetProperties.updateNestedProperties(nestedValues);
    }

    @Override
    public InputStream getWizardPngImage(String wizardName, WizardImageType imageType) {
        ComponentWizardDefinition wizardDefinition = componentRegistry.getComponentWizards().get(wizardName);
        if (wizardDefinition != null) {
            return getImageStream(wizardDefinition, wizardDefinition.getPngImagePath(imageType));
        } else {
            throw new ComponentException(ComponentsApiErrorCode.WRONG_WIZARD_NAME,
                    ExceptionContext.build().put("name", wizardName)); //$NON-NLS-1$
        }

    }

    @Override
    public InputStream getComponentPngImage(String componentName, ComponentImageType imageType) {
        ComponentDefinition componentDefinition = getComponentDefinition(componentName);
        return getImageStream(componentDefinition, componentDefinition.getPngImagePath(imageType));
    }

    /**
     * get the image stream or null
     * 
     * @param definition, must not be null
     * @return the stream or null if no image was defined for th component or the path is wrong
     */
    private InputStream getImageStream(NamedThing definition, String pngIconPath) {
        InputStream result = null;
        if (pngIconPath != null && !"".equals(pngIconPath)) { //$NON-NLS-1$
            InputStream resourceAsStream = definition.getClass().getResourceAsStream(pngIconPath);
            if (resourceAsStream == null) {// no resource found so this is an component error, so log it and return
                                           // null
                LOGGER.error("Failed to load the Wizard icon [" + definition.getName() + "," + pngIconPath + "]"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            } else {
                result = resourceAsStream;
            }
        } else {// no path provided so will return null but log it.
            LOGGER.warn("The defintion of [" + definition.getName() + "] did not specify any icon"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        return result;
    }

    @Override
    public RuntimeInfo getRuntimeInfo(String componentName, Properties properties, ConnectorTopology componentType) {
        ComponentDefinition componentDef = getComponentDefinition(componentName);
        return componentDef.getRuntimeInfo(properties, componentType);

    }

    private java.util.Properties toProperties(Map<String, String> dominant, Map<String, String> recessive) {
        java.util.Properties props = new java.util.Properties();
        if (recessive != null) {
            props.putAll(recessive);
        }
        if (dominant != null) {
            props.putAll(dominant);
        }
        return props;
    }

    @Override
    public Schema getSchema(ComponentProperties cp, Connector connector, boolean isOutputConnection) {
        return cp.getSchema(connector, isOutputConnection);
    }

    @Override
    public Set<? extends Connector> getAvailableConnectors(ComponentProperties componentProperties,
            Set<? extends Connector> connectedConnetor, boolean isOuput) {
        return componentProperties.getAvailableConnectors(connectedConnetor, isOuput);
    }

    @Override
    public void setSchema(ComponentProperties componentProperties, Connector connector, Schema schema, boolean isOuput) {
        componentProperties.setConnectedSchema(connector, schema, isOuput);
    }

}
