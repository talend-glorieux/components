package ${package};

import javax.inject.Inject;

import org.junit.Test;
import org.talend.components.api.service.ComponentService;
import org.talend.components.api.test.AbstractComponentTest;
import org.springframework.beans.factory.annotation.Qualifier;

public class ${componentName}TestBase extends AbstractComponentTest {

    @Inject
    @Qualifier(value = "baseComponentService")
    private ComponentService componentService;

    public ComponentService getComponentService(){
        return componentService;
    }
    
    @Test
    public void componentHasBeenRegistered(){
        assertComponentIsRegistered("${componentName}");
    }
}
