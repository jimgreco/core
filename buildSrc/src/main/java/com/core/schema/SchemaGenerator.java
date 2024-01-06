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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@SuppressWarnings({ "checkstyle:MissingJavadocType", "checkstyle:MissingJavadocMethod" })
public class SchemaGenerator {

    public static void generate(String inputFile, Path root) throws Exception {
        var schema = parseSchema(inputFile);

        var engine = new VelocityEngine();
        engine.setProperty("resource.loader.class.class",
                "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
        engine.setProperty("resource.loaders", "class");

        engine.init();

        var context = new VelocityContext();
        context.put("schema", schema);

        var directory = root.resolve(schema.getPackage().replace('.', File.separatorChar));
        directory.toFile().mkdirs();

        for (var theEnum : schema.enums) {
            context.put("enum", theEnum);
            write(engine, context, directory, "enum.vm", theEnum.getName() + ".java");
        }

        for (var message : schema.messages) {
            context.put("message", message);
            write(engine, context, directory, "decoder.vm", message.getNameCap() + "Decoder.java");
            write(engine, context, directory, "encoder.vm", message.getNameCap() + "Encoder.java");
        }

        write(engine, context, directory, "schema.vm", schema.getPrefix() + "Schema.java");
        write(engine, context, directory, "dispatcher.vm", schema.getPrefix() + "Dispatcher.java");
        write(engine, context, directory, "provider.vm", schema.getPrefix() + "Provider.java");
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

        var headerElement = document.getElementsByTagName("header").item(0);
        var header = new Message(
                (byte) 0, "header", null, null, null, null, "");
        addFields((Element) headerElement, header, true);

        var schema = new Schema(
                docElement.getAttribute("prefix"),
                docElement.getAttribute("package"),
                Integer.parseInt(docElement.getAttribute("version")),
                docElement.getAttribute("description"),
                header);

        var propertyElements = document.getElementsByTagName("property");
        for (var i = 0; i < propertyElements.getLength(); i++) {
            var propertyElement = (Element) propertyElements.item(i);
            schema.properties.put(propertyElement.getAttribute("name"), propertyElement.getAttribute("value"));
        }

        var enumElements = document.getElementsByTagName("enum");
        for (var i = 0; i < enumElements.getLength(); i++) {
            var enumElement = (Element) enumElements.item(i);
            var theEnum = new Enum(
                    enumElement.getAttribute("name"),
                    enumElement.getAttribute("description"));
            schema.enums.add(theEnum);
            var valueElements = enumElement.getElementsByTagName("value");
            for (var j = 0; j < valueElements.getLength(); j++) {
                var valueElement = (Element) valueElements.item(j);
                theEnum.values.add(new Value(
                        valueElement.getAttribute("name"),
                        Byte.parseByte(valueElement.getAttribute("value")),
                        valueElement.getAttribute("description")));
            }
        }

        var messageElements = document.getElementsByTagName("message");
        for (var i = 0; i < messageElements.getLength(); i++) {
            var messageElement = (Element) messageElements.item(i);
            var entity = messageElement.getAttribute("entity");
            var baseEntity = messageElement.getAttribute("base-entity");
            var message = new Message(
                    Integer.parseInt(messageElement.getAttribute("id")),
                    messageElement.getAttribute("name"),
                    entity.isBlank() ? null : entity,
                    baseEntity.isBlank() ? null : baseEntity,
                    messageElement.getAttribute("decoderInterface"),
                    messageElement.getAttribute("encoderInterface"),
                    messageElement.getAttribute("description"));
            message.requiredFields.addAll(header.requiredFields);
            addFields(messageElement, message, false);
            schema.messages.add(message);
        }

        return schema;
    }

    private static void addFields(Element messageElement, Message message, boolean header) {
        var requiredFields = messageElement.getElementsByTagName("field");
        for (var i = 0; i < requiredFields.getLength(); i++) {
            addField(message, message.requiredFields, (Element) requiredFields.item(i), header, true);
        }

        var optionalFields = messageElement.getElementsByTagName("optional");
        for (var i = 0; i < optionalFields.getLength(); i++) {
            addField(message, message.optionalFields, (Element) optionalFields.item(i), header, false);
        }
    }

    private static void addField(
            Message message, List<Field> fields, Element fieldElement, boolean header, boolean required) {
        var version = fieldElement.getAttribute("version");
        var primaryKey = fieldElement.getAttribute("primary-key");
        var key = fieldElement.getAttribute("key");
        var id = fieldElement.getAttribute("id");
        var metadata = fieldElement.getAttribute("metadata");
        var impliedDecimals = fieldElement.getAttribute("implied-decimals");
        var foreignKey = fieldElement.getAttribute("foreign-key");
        fields.add(new Field(
                id.isBlank() ? 0 : Byte.parseByte(id),
                fieldElement.getAttribute("name"),
                fieldElement.getAttribute("type"),
                fieldElement.getAttribute("description"),
                required,
                header,
                version.isBlank() ? 1 : Byte.parseByte(version),
                metadata.isBlank() ? "null" : '"' + metadata + '"',
                impliedDecimals.isBlank() ? 0 : Integer.parseInt(impliedDecimals),
                !primaryKey.isBlank() && Boolean.parseBoolean(primaryKey),
                !key.isBlank() && Boolean.parseBoolean(key),
                foreignKey.isBlank() ? "null" : '"' + foreignKey + '"',
                message.getSize()));
    }

    public static class Enum {

        private final String name;
        private final List<Value> values;
        private final String description;

        private Enum(String name, String description) {
            this.name = name.substring(0, 1).toUpperCase(Locale.ROOT) + name.substring(1);
            if (description.isBlank()) {
                this.description = description;
            } else {
                this.description = description.substring(0, 1).toLowerCase(Locale.ROOT) + description.substring(1);
            }
            values = new ArrayList<>();
        }

        public String getName() {
            return name;
        }

        public List<Value> getValues() {
            return values;
        }

        public String getDescription() {
            return description;
        }
    }

    public static class Value {

        private final byte value;
        private final String name;
        private final String description;

        private Value(String name, byte value, String description) {
            this.value = value;
            // converts capitalized words into upper case words separated by underscores
            // i.e., PostOnly => POST_ONLY
            var theName = "";
            for (var i = 0; i < name.length(); i++) {
                var character = name.charAt(i);
                if (i != 0 && Character.isUpperCase(character)) {
                    theName += "_";
                }
                theName += Character.toUpperCase(character);
            }
            this.name = theName;
            if (description.isBlank()) {
                this.description = description;
            } else {
                this.description = description.substring(0, 1).toUpperCase(Locale.ROOT) + description.substring(1);
            }
        }

        public byte getValue() {
            return value;
        }

        public String getName() {
            return name;
        }

        public String getDescription() {
            return description;
        }
    }

    public static class Message {

        private final int id;
        private final String name;
        private final String nameCap;
        private final String entity;
        private final String baseEntity;
        private final String decoderInterface;
        private final String encoderInterface;
        private final List<Field> requiredFields;
        private final List<Field> optionalFields;
        private final String description;

        private Message(
                int id, String name, String entity, String baseEntity,
                String decoderInterface, String encoderInterface, String description) {
            this.id = id;
            this.name = name.substring(0, 1).toLowerCase(Locale.ROOT) + name.substring(1);
            this.nameCap = name.substring(0, 1).toUpperCase(Locale.ROOT) + name.substring(1);
            this.entity = entity == null ? null : entity.substring(0, 1).toLowerCase(Locale.ROOT) + entity.substring(1);
            this.baseEntity = baseEntity == null ? null
                    : baseEntity.substring(0, 1).toLowerCase(Locale.ROOT) + baseEntity.substring(1);
            this.decoderInterface = decoderInterface;
            this.encoderInterface = encoderInterface;
            if (description.isBlank()) {
                this.description = description;
            } else {
                this.description = description.substring(0, 1).toLowerCase(Locale.ROOT) + description.substring(1);
            }
            requiredFields = new ArrayList<>();
            optionalFields = new ArrayList<>();
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getNameCap() {
            return nameCap;
        }

        public String getEntity() {
            return entity;
        }

        public String getBaseEntity() {
            return baseEntity;
        }

        public String getDecoderInterface() {
            return decoderInterface;
        }

        public String getEncoderInterface() {
            return encoderInterface;
        }

        public List<Field> getRequiredFields() {
            return requiredFields;
        }

        public List<Field> getOptionalFields() {
            return optionalFields;
        }

        public List<Field> getFields() {
            var allFields = new ArrayList<>(requiredFields);
            allFields.addAll(optionalFields);
            return allFields;
        }

        public int getSize() {
            return requiredFields.stream().mapToInt(x -> x.size).sum();
        }

        public String getDescription() {
            return description;
        }
    }

    public static class Field {

        private final byte id;
        private final String name;
        private final String nameCap;
        private final String type;
        private final String typeCap;
        private final String foreignKey;
        private final boolean required;
        private final boolean header;
        private final boolean primitive;
        private final String metadata;
        private final int impliedDecimals;
        private final boolean primaryKey;
        private final boolean key;
        private final int offset;
        private final int size;
        private final byte version;
        private final String description;

        private Field(
                byte id, String name, String type, String description, boolean required, boolean header, byte version,
                String metadata, int impliedDecimals, boolean primaryKey, boolean key, String foreignKey, int offset) {
            this.id = id;
            this.name = name.substring(0, 1).toLowerCase(Locale.ROOT) + name.substring(1);
            this.nameCap = name.substring(0, 1).toUpperCase(Locale.ROOT) + name.substring(1);
            this.type = type;
            this.typeCap = type.substring(0, 1).toUpperCase(Locale.ROOT) + type.substring(1);
            if (description.isBlank()) {
                this.description = description;
            } else {
                this.description = description.substring(0, 1).toLowerCase(Locale.ROOT) + description.substring(1);
            }
            this.required = required;
            this.header = header;
            this.version = version;
            this.offset = offset;
            this.metadata = metadata;
            this.impliedDecimals = impliedDecimals;
            this.primaryKey = primaryKey;
            this.key = key;
            this.foreignKey = foreignKey;
            switch (type) {
                case "byte":
                    size = 1;
                    primitive = true;
                    break;
                case "char":
                case "short":
                    size = 2;
                    primitive = true;
                    break;
                case "int":
                case "float":
                    size = 4;
                    primitive = true;
                    break;
                case "long":
                case "double":
                    size = 8;
                    primitive = true;
                    break;
                case "DirectBuffer":
                    size = 0;
                    primitive = false;
                    break;
                default:
                    size = 1;
                    primitive = false;
            }
        }

        public byte getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getNameCap() {
            return nameCap;
        }

        public String getType() {
            return type;
        }

        public String getTypeCap() {
            return typeCap;
        }

        public boolean isRequired() {
            return required;
        }

        public boolean isHeader() {
            return header;
        }

        public int getOffset() {
            return offset;
        }

        public boolean isPrimitive() {
            return primitive;
        }

        public byte getVersion() {
            return version;
        }

        public String getForeignKey() {
            return foreignKey;
        }

        public boolean isPrimaryKey() {
            return primaryKey;
        }

        public boolean isKey() {
            return key;
        }

        public String getMetadata() {
            return metadata;
        }

        public int getImpliedDecimals() {
            return impliedDecimals;
        }

        public String getDescription() {
            return description;
        }

        public int getSize() {
            return size;
        }
    }

    public static class Schema {

        private final String prefix;
        private final String pkg;
        private final int version;
        private final Message header;
        private final List<Message> messages;
        private final List<Enum> enums;
        private final Map<String, String> properties;
        private final String description;

        private Schema(String prefix, String pkg, int version, String description, Message header) {
            this.prefix = prefix;
            this.pkg = pkg;
            this.version = version;
            if (description.isBlank()) {
                this.description = description;
            } else {
                this.description = description.substring(0, 1).toLowerCase(Locale.ROOT) + description.substring(1);
            }
            this.header = header;
            messages = new ArrayList<>();
            enums = new ArrayList<>();
            properties = new LinkedHashMap<>();
        }

        public String getPrefix() {
            return prefix;
        }

        public String getPackage() {
            return pkg;
        }

        public int getVersion() {
            return version;
        }

        public List<Message> getMessages() {
            return messages;
        }

        public Message getHeader() {
            return header;
        }

        public int headerOffset(String name) {
            return header.requiredFields.stream().filter(x -> x.name.equals(name)).findFirst().get().offset;
        }

        public String getDescription() {
            return description;
        }

        public Map<String, String> getProperties() {
            return properties;
        }
    }
}
