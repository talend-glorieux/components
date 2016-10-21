package org.talend.components.service.rest.configuration;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.List;

import org.slf4j.Logger;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.talend.components.service.rest.serialization.JsonSchema2HttpMessageConverter;

/**
 * Setup SpringMVC
 */
@Configuration
public class MvcSetup extends WebMvcConfigurerAdapter {

    /** This class' logger. */
    private static final Logger LOGGER = getLogger(MvcSetup.class);

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        converters.add(new JsonSchema2HttpMessageConverter());
        LOGGER.info("adding custom jsonSchema converter to converters");
    }

}
