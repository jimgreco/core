package com.core.schema;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

@SuppressWarnings({ "checkstyle:MissingJavadocType", "checkstyle:MissingJavadocMethod" })
public class FixGenerator {

    public static void generate(String schemaXml, String outputDir) throws Exception {
        final var schema = parseSchema(schemaXml);
        final var root = Path.of(outputDir);

        var engine = new VelocityEngine();
        engine.setProperty("resource.loader.class.class",
                "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
        engine.setProperty("resource.loaders", "class");
        engine.init();

        var context = new VelocityContext();
        context.put("schema", schema);

        var directory = root.resolve(schema.getPackage().replace('.', File.separatorChar));
        directory.toFile().mkdirs();

        for (var theEnum : schema.getEnumFields()) {
            context.put("enum", theEnum);
            write(engine, context, directory, "/fix-enum.vm", theEnum.getName() + ".java");
        }

        write(engine, context, directory, "/fix-schema.vm", schema.getName() + ".java");
    }

    private static void write(
            VelocityEngine engine, VelocityContext context, Path directory, String template, String fileName)
            throws Exception {
        var velocityTemplate = engine.getTemplate(template);
        var writer = new StringWriter();
        velocityTemplate.merge(context, writer);
        Files.writeString(directory.resolve(fileName), writer.toString(), StandardOpenOption.CREATE);
    }

    private static Schema parseSchema(String arg) throws SAXException, IOException, ParserConfigurationException {
        var document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(arg);
        var docElement = document.getDocumentElement();

        var schema = new Schema(docElement.getAttribute("package"), docElement.getAttribute("name"));
        var fieldElements = document.getElementsByTagName("field");
        for (var i = 0; i < fieldElements.getLength(); i++) {
            var fieldElement = (Element) fieldElements.item(i);
            var field = new Field(
                    fieldElement.getAttribute("name"),
                    Integer.parseInt(fieldElement.getAttribute("number")),
                    fieldElement.getAttribute("type"));
            schema.fields.add(field);

            var valueElements = fieldElement.getElementsByTagName("value");
            for (var j = 0; j < valueElements.getLength(); j++) {
                var valueElement = (Element) valueElements.item(j);
                field.values.add(new Value(
                        valueElement.getAttribute("description"),
                        valueElement.getAttribute("enum")));
            }
        }

        return schema;
    }

    public static class Schema {

        private final String pkg;
        private final String name;
        private final List<Field> fields;

        private Schema(String pkg, String name) {
            this.pkg = pkg;
            this.name = name;
            fields = new ArrayList<>();
        }

        public List<Field> getEnumFields() {
            return fields.stream().filter(x -> !x.values.isEmpty()).collect(Collectors.toList());
        }

        public List<Field> getFields() {
            return fields;
        }

        public String getPackage() {
            return pkg;
        }

        public String getName() {
            return name;
        }
    }

    public static class Field {

        private final String name;
        private final String nameCap;
        private final int number;
        private final String type;
        private final List<Value> values;

        private Field(String name, int number, String type) {
            this.name = name.substring(0, 1).toUpperCase(Locale.ROOT) + name.substring(1);
            this.number = number;
            this.type = type;
            values = new ArrayList<>();

            var nameCap = "";
            var firstCapitalLetterIndex = 0;

            for (var i = 0; i < name.length(); i++) {
                var c = name.charAt(i);
                if (c >= 'A' && c <= 'Z') {
                    if (firstCapitalLetterIndex == -1) {
                        nameCap += "_";
                        firstCapitalLetterIndex = nameCap.length();
                    }
                    nameCap += c;
                } else {
                    var lastCharIndex = nameCap.length() - 1;
                    if (firstCapitalLetterIndex != -1 && firstCapitalLetterIndex != lastCharIndex) {
                        nameCap = nameCap.substring(0, lastCharIndex) + "_" + nameCap.charAt(lastCharIndex);
                    }
                    if (c >= 'a' && c <= 'z') {
                        nameCap += (char) (c - 0x20);
                    } else {
                        nameCap += c;
                    }
                    firstCapitalLetterIndex = -1;
                }
            }

            this.nameCap = nameCap;
        }

        public String getNameCap() {
            return nameCap;
        }

        public String getName() {
            return name;
        }

        public List<Value> getValues() {
            return values;
        }

        public int getNumber() {
            return number;
        }

        public String getType() {
            return type;
        }

        public boolean isSingleCharacter() {
            return values.stream().noneMatch(x -> x.value.length() > 1);
        }
    }

    public static class Value {

        private final String name;
        private final String value;

        private Value(String name, String value) {
            var upperName = name.toUpperCase(Locale.ROOT);
            if (upperName.charAt(0) < 'A' || upperName.charAt(0) > 'Z') {
                upperName = '_' + upperName;
            }
            this.name = upperName;
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public String getName() {
            return name;
        }
    }
}
