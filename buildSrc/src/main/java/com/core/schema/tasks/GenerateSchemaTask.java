package com.core.schema.tasks;

import com.core.schema.SchemaGenerator;
import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;

import java.nio.file.Path;

/**
 * Gradle task to run Core schema generator.
 */
public class GenerateSchemaTask extends DefaultTask {

    private String schemaXml;
    private String outputDir;

    /**
     * Get the schema xml file.
     * @return the schema xml file
     */
    @InputFile
    public String getSchemaXml() {
        return this.schemaXml;
    }

    /**
     * Set the schema xml file.
     * @param schemaXml the schema xml file
     */
    public void setSchemaXml(String schemaXml) {
        this.schemaXml = schemaXml;
    }

    /**
     * Get the schema output directory.
     * @return the schema output directory
     */
    @OutputDirectory
    public String getOutputDir() { return this.outputDir; }

    /**
     * Set the schema output directory.
     * @param outputDir the schema output directory
     */
    public void setOutputDir(String outputDir) { this.outputDir = outputDir; }

    /**
     * Run the task action to generate the schema.
     */
    @TaskAction
    public void generate() throws Exception {
        SchemaGenerator.generate(getSchemaXml(), Path.of(getOutputDir()));
    }
}
