package org.talend.components.api.service.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;
import org.talend.daikon.properties.Properties;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JsonUtilTest {

    @Test
    public void test() throws URISyntaxException, IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        String jsonStr = readJson("TestComponentProperties.json");

        Properties properties = JsonUtil.fromJson(jsonStr);
        properties.init();

        String jsonResult = JsonUtil.toJson(properties, true);
        assertEquals(jsonStr, jsonResult);
    }

    private static String readJson(String path) throws URISyntaxException, IOException {
        java.net.URL url = JsonUtilTest.class.getResource(path);
        java.nio.file.Path resPath = java.nio.file.Paths.get(url.toURI());
        return new String(java.nio.file.Files.readAllBytes(resPath), "UTF8");
    }

    private static final ObjectMapper mapper = new ObjectMapper();

    private static String readJson(Map<String, String> keyAndPath) throws IOException,
            URISyntaxException {
        ObjectNode objectNode = mapper.createObjectNode();
        for (String key : keyAndPath.keySet()) {
            objectNode.set(key, mapper.readTree(readJson(keyAndPath.get(key))));
        }
        return objectNode.toString();
    }

}
