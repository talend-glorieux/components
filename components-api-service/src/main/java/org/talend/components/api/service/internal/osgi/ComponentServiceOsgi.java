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
package org.talend.components.api.service.internal.osgi;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

import org.apache.avro.Schema;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.component.ComponentDefinition;
import org.talend.components.api.component.ComponentImageType;
import org.talend.components.api.component.Connector;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.api.service.ComponentService;
import org.talend.components.api.service.internal.ComponentRegistry;
import org.talend.components.api.service.internal.ComponentServiceImpl;
import org.talend.components.api.wizard.ComponentWizard;
import org.talend.components.api.wizard.ComponentWizardDefinition;
import org.talend.components.api.wizard.WizardImageType;
import org.talend.daikon.NamedThing;
import org.talend.daikon.i18n.GlobalI18N;
import org.talend.daikon.properties.Properties;
import org.talend.daikon.properties.service.Repository;

import aQute.bnd.annotation.component.Activate;
import aQute.bnd.annotation.component.Component;
import aQute.bnd.annotation.component.Reference;

/**
 * This is the OSGI specific service implementation that completely delegates the implementation to the Framework
 * agnostic {@link ComponentServiceImpl}
 */
@Component
public class ComponentServiceOsgi implements ComponentService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComponentServiceOsgi.class);

    GlobalI18N gctx;

    @Reference
    public void osgiInjectGlobalContext(GlobalI18N aGctx) {
        this.gctx = aGctx;
    }

    private final class ComponentRegistryOsgi implements ComponentRegistry {

        private BundleContext bc;

        public ComponentRegistryOsgi(BundleContext bc) {
            this.bc = bc;

        }

        private Map<String, ComponentDefinition> components;

        private Map<String, ComponentWizardDefinition> componentWizards;

        protected <T extends NamedThing> Map<String, T> populateMap(Class<T> cls) {
            Map<String, T> map = new HashMap<>();
            try {
                String typeCanonicalName = cls.getCanonicalName();
                Collection<ServiceReference<T>> serviceReferences = bc.getServiceReferences(cls, null);
                for (ServiceReference<T> sr : serviceReferences) {
                    T service = bc.getService(sr);
                    Object nameProp = sr.getProperty("component.name"); //$NON-NLS-1$
                    if (nameProp instanceof String) {
                        map.put((String) nameProp, service);
                        LOGGER.info("Registered the component: " + nameProp + "(" + service.getClass().getCanonicalName() + ")"); //$NON-NLS-1$//$NON-NLS-2$//$NON-NLS-3$
                    } else {// no name set so issue a warning
                        LOGGER.warn("Failed to register the following component because it is unnamed: " //$NON-NLS-1$
                                + service.getClass().getCanonicalName());
                    }
                }
                if (map.isEmpty()) {// warn if not comonents where registered
                    LOGGER.warn("Could not find any registered components for type :" + typeCanonicalName); //$NON-NLS-1$
                } // else everything is fine
            } catch (InvalidSyntaxException e) {
                LOGGER.error("Failed to get ComponentDefinition services", e); //$NON-NLS-1$
            }
            return map;
        }

        @Override
        public Map<String, ComponentDefinition> getComponents() {
            if (components == null) {
                components = populateMap(ComponentDefinition.class);
            }
            return components;
        }

        @Override
        public Map<String, ComponentWizardDefinition> getComponentWizards() {
            if (componentWizards == null) {
                componentWizards = populateMap(ComponentWizardDefinition.class);
            }
            return componentWizards;
        }
    }

    private ComponentService componentServiceDelegate;

    @Activate
    void activate(BundleContext bundleContext) throws InvalidSyntaxException {
        this.componentServiceDelegate = new ComponentServiceImpl(new ComponentRegistryOsgi(bundleContext));
    }

    @Override
    public ComponentProperties getComponentProperties(String name) {
        return componentServiceDelegate.getComponentProperties(name);
    }

    @Override
    public ComponentDefinition getComponentDefinition(String name) {
        return componentServiceDelegate.getComponentDefinition(name);
    }

    @Override
    public ComponentWizard getComponentWizard(String name, String userData) {
        return componentServiceDelegate.getComponentWizard(name, userData);
    }

    @Override
    public List<ComponentWizard> getComponentWizardsForProperties(ComponentProperties properties, String location) {
        return componentServiceDelegate.getComponentWizardsForProperties(properties, location);
    }

    @Override
    public List<ComponentDefinition> getPossibleComponents(ComponentProperties... properties) throws Throwable {
        return componentServiceDelegate.getPossibleComponents(properties);
    }

    @Override
    public Properties makeFormCancelable(Properties properties, String formName) {
        return componentServiceDelegate.makeFormCancelable(properties, formName);
    }

    @Override
    public Properties cancelFormValues(Properties properties, String formName) {
        return componentServiceDelegate.cancelFormValues(properties, formName);
    }

    public ComponentProperties commitFormValues(ComponentProperties properties, String formName) {
        // FIXME - remove this
        return properties;
    }

    @Override
    public Properties validateProperty(String propName, Properties properties) throws Throwable {
        return componentServiceDelegate.validateProperty(propName, properties);
    }

    @Override
    public Properties beforePropertyActivate(String propName, Properties properties) throws Throwable {
        return componentServiceDelegate.beforePropertyActivate(propName, properties);
    }

    @Override
    public Properties beforePropertyPresent(String propName, Properties properties) throws Throwable {
        return componentServiceDelegate.beforePropertyPresent(propName, properties);
    }

    @Override
    public Properties afterProperty(String propName, Properties properties) throws Throwable {
        return componentServiceDelegate.afterProperty(propName, properties);
    }

    @Override
    public Properties beforeFormPresent(String formName, Properties properties) throws Throwable {
        return componentServiceDelegate.beforeFormPresent(formName, properties);
    }

    @Override
    public Properties afterFormNext(String formName, Properties properties) throws Throwable {
        return componentServiceDelegate.afterFormNext(formName, properties);
    }

    @Override
    public Properties afterFormBack(String formName, Properties properties) throws Throwable {
        return componentServiceDelegate.afterFormBack(formName, properties);
    }

    @Override
    public Properties afterFormFinish(String formName, Properties properties) throws Throwable {
        return componentServiceDelegate.afterFormFinish(formName, properties);
    }

    @Override
    public Set<String> getAllComponentNames() {
        return componentServiceDelegate.getAllComponentNames();
    }

    @Override
    public Set<ComponentDefinition> getAllComponents() {
        return componentServiceDelegate.getAllComponents();
    }

    @Override
    public Set<ComponentWizardDefinition> getTopLevelComponentWizards() {
        return componentServiceDelegate.getTopLevelComponentWizards();
    }

    @Override
    public InputStream getWizardPngImage(String wizardName, WizardImageType imageType) {
        return componentServiceDelegate.getWizardPngImage(wizardName, imageType);
    }

    @Override
    public InputStream getComponentPngImage(String componentName, ComponentImageType imageType) {
        return componentServiceDelegate.getComponentPngImage(componentName, imageType);
    }

    @Override
    public String storeProperties(Properties properties, String name, String repositoryLocation, String schemaPropertyName) {
        return componentServiceDelegate.storeProperties(properties, name, repositoryLocation, schemaPropertyName);
    }

    @Override
    public void setRepository(Repository repository) {
        componentServiceDelegate.setRepository(repository);
    }

    @Override
    public Set<String> getMavenUriDependencies(String componentName) {
        return componentServiceDelegate.getMavenUriDependencies(componentName);
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
    public String setupComponentRuntime(String name, String type, ComponentProperties properties) {
        return componentServiceDelegate.setupComponentRuntime(name, type, properties);
    }


    @Override
    public void readRuntimeInput(String name, String type, String id, OutputStream output) {
        componentServiceDelegate.readRuntimeInput(name, type, id, output);
    }
}
