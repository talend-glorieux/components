package ${package};

import static org.ops4j.pax.exam.CoreOptions.*;

import javax.inject.Inject;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.talend.components.api.ComponentsPaxExamOptions;
import org.talend.components.api.service.ComponentService;
import org.talend.components.api.test.ComponentTestUtils;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class Osgi${componentName}TestIT extends ${componentName}TestBase {

    @Configuration
    public Option[] config() {

        return options(composite(ComponentsPaxExamOptions.getOptions()), //
                linkBundle("org.talend.components-components-common-bundle"), //
                linkBundle("${groupId}-${artifactId}-bundle"));
    }

}
