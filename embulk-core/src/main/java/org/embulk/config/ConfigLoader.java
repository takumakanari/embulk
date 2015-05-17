package org.embulk.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import com.google.inject.Inject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.jruby.embed.ScriptingContainer;
import org.yaml.snakeyaml.Yaml;

public class ConfigLoader
{
    private final ModelManager model;

    @Inject
    public ConfigLoader(ModelManager model)
    {
        this.model = model;
    }

    public ConfigSource fromJson(JsonParser parser) throws IOException
    {
        // TODO check parsed.isObject()
        ObjectNode source = (ObjectNode) new ObjectMapper().readTree(parser);
        return new DataSourceImpl(model, source);
    }

    public ConfigSource fromYamlFile(File path) throws IOException
    {
        Yaml yaml = new Yaml();
        Object parsedYaml;
        try (FileInputStream is = new FileInputStream(path)) {
            parsedYaml = yaml.load(is);
        }
        ObjectNode source = objectToJsonObject(parsedYaml);
        ConfigSource config = new DataSourceImpl(model, source);

        return config;
    }

    public ConfigSource fromRubyDSLFile(ScriptingContainer jruby, String configString)
    {
        Object rubyDslParser = jruby.runScriptlet("Embulk::DSLParser");
        Object dslObject = jruby.callMethod(rubyDslParser, "parse", configString);
        ObjectNode source = objectToJsonObject(dslObject);

        final ConfigSource config = new DataSourceImpl(model, source);
        return config;
    }

    public ConfigSource fromPropertiesYamlLiteral(Properties props, String keyPrefix)
    {
        // TODO exception handling
        ObjectNode source = new ObjectNode(JsonNodeFactory.instance);
        Yaml yaml = new Yaml();
        for (String key : props.stringPropertyNames()) {
            // TODO handle "." and "[...]" as map and array acccessor for example:
            //      in.parser.type=csv => {"in": {"parser": {"type": "csv"}}}
            if (!key.startsWith(keyPrefix)) {
                continue;
            }
            String yamlValue = props.getProperty(key);
            String keyName = key.substring(keyPrefix.length());
            Object parsedValue = yaml.load(yamlValue);
            JsonNode typedValue = objectToJson(parsedValue);
            source.set(keyName, typedValue);
        }
        return new DataSourceImpl(model, source);
    }

    private JsonNode objectToJson(Object object)
    {
        // TODO exception
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readTree(objectMapper.writeValueAsString(object));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private ObjectNode objectToJsonObject(Object object)
    {
        // TODO exception
        JsonNode json = objectToJson(object);
        if (!json.isObject()) {
            throw new RuntimeException("Expected object to deserialize ConfigSource but got "+json);
        }
        return (ObjectNode) json;
    }
}
