package org.talend.components.multiruntime;

import javax.inject.Inject;

import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.talend.components.api.service.ComponentService;
import org.talend.components.service.spring.SpringTestApp;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringTestApp.class)
public class SpringMultiRuntimeComponentTestIT extends AbstractMultiRuntimeComponentTests {

    @Inject
    @Qualifier(value = "baseComponentService")
    private ComponentService componentService;

    @Override
    public ComponentService getComponentService() {
        return componentService;
    }

}
