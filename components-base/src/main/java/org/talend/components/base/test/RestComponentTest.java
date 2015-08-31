package org.talend.components.base.test;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.springframework.web.client.RestTemplate;

import java.util.LinkedHashMap;
import java.util.List;

public class RestComponentTest extends TestCase {

    public static final String SERVER_URI = "http://localhost:8080/components/";

    public RestComponentTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(RestComponentTest.class);
    }

    public void testApp() {
        RestTemplate restTemplate = new RestTemplate();
        //we can't get List<Employee> because JSON convertor doesn't know the type of
        //object in the list and hence convert it to default JSON object type LinkedHashMap
        List<LinkedHashMap> emps = restTemplate.getForObject(SERVER_URI + "TestComponent/properties", List.class);
        System.out.println(emps.size());
        for (LinkedHashMap map : emps) {
            System.out.println("ID=" + map.get("id") + ",Name=" + map.get("name") + ",CreatedDate=" + map.get("createdDate"));
            ;
        }
    }
}