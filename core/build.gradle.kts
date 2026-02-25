/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import com.github.autostyle.gradle.AutostyleTask
import com.github.vlsi.gradle.crlf.CrLfSpec
import com.github.vlsi.gradle.crlf.LineEndings
import com.github.vlsi.gradle.ide.dsl.settings
import com.github.vlsi.gradle.ide.dsl.taskTriggers

plugins {
    kotlin("jvm")
    id("com.github.vlsi.crlf")
    id("com.github.vlsi.ide")
    calcite.fmpp
    calcite.javacc
}

val integrationTestConfig: (Configuration.() -> Unit) = {
    isCanBeConsumed = false
    isTransitive = true
    extendsFrom(configurations.testRuntimeClasspath.get())
}

// The custom configurations below allow to include dependencies (and jars) in the classpath only
// when IT tests are running. In the future it may make sense to include the JDBC driver
// dependencies using the default 'testRuntimeOnly' configuration to simplify the build but at the
// moment they can remain as is.
val testH2 by configurations.creating(integrationTestConfig)
val testOracle by configurations.creating(integrationTestConfig)
val testPostgresql by configurations.creating(integrationTestConfig)
val testMysql by configurations.creating(integrationTestConfig)

dependencies {
    api(project(":linq4j"))

    api("org.locationtech.jts:jts-core")
    api("org.locationtech.jts.io:jts-io-common")
    api("org.locationtech.proj4j:proj4j")
    api("com.fasterxml.jackson.core:jackson-annotations")
    api("com.google.errorprone:error_prone_annotations")
    api("com.google.guava:guava")
    api("org.apache.calcite.avatica:avatica-core")
    api("org.apiguardian:apiguardian-api")
    api("org.checkerframework:checker-qual")
    api("org.slf4j:slf4j-api")

    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml")
    implementation("com.google.uzaygezen:uzaygezen-core") {
        exclude("log4j", "log4j").because("conflicts with log4j-slf4j-impl which uses log4j2 and" +
                " also leaks transitively to projects depending on calcite-core")
    }
    implementation("com.jayway.jsonpath:json-path")
    implementation("com.yahoo.datasketches:sketches-core")
    implementation("commons-codec:commons-codec")
    implementation("net.hydromatic:aggdesigner-algorithm")
    implementation("org.apache.commons:commons-dbcp2")
    implementation("org.apache.commons:commons-lang3")
    implementation("org.apache.commons:commons-math3")
    implementation("org.apache.commons:commons-text")
    implementation("org.jooq:joou-java-6")
    implementation("commons-io:commons-io")
    implementation("org.codehaus.janino:commons-compiler")
    implementation("org.codehaus.janino:janino")
    annotationProcessor("org.immutables:value")
    compileOnly("org.immutables:value-annotations")
    compileOnly("com.google.code.findbugs:jsr305")
    testAnnotationProcessor("org.immutables:value")
    testCompileOnly("org.immutables:value-annotations")
    testCompileOnly("com.google.code.findbugs:jsr305")

    testH2("com.h2database:h2")
    testMysql("mysql:mysql-connector-java")
    testOracle("com.oracle.ojdbc:ojdbc8")
    testPostgresql("org.postgresql:postgresql")

    testImplementation(project(":testkit"))
    testImplementation("net.bytebuddy:byte-buddy")
    testImplementation("net.hydromatic:foodmart-queries")
    testImplementation("net.hydromatic:quidem")
    testImplementation("org.apache.calcite.avatica:avatica-server")
    testImplementation("org.apache.commons:commons-pool2")
    testImplementation("org.hsqldb:hsqldb::jdk8")
    testImplementation("sqlline:sqlline")
    testImplementation(kotlin("stdlib-jdk8"))
    testImplementation(kotlin("test"))
    testImplementation(kotlin("test-junit5"))

    // proj4j-epsg must not be converted to 'implementation' due to its license
    testRuntimeOnly("org.locationtech.proj4j:proj4j-epsg")

    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl")
    testRuntimeOnly(project(":plus"))
}

tasks.jar {
    CrLfSpec(LineEndings.LF).run {
        into("codegen") {
            textFrom("$projectDir/src/main/codegen")
        }
    }
}

val generatedVersionDir = layout.buildDirectory.get().file("generated/sources/version")
val versionClass by tasks.registering(Sync::class) {
    val re = Regex("^(\\d+)\\.(\\d+).*")

    val version = project.version.toString()
    val matchResult = re.find(version) ?: throw GradleException("Unable to parse major.minor version parts from project.version '$version'")
    val (major, minor) = matchResult.destructured

    // This makes Gradle re-execute the task when version is updated
    inputs.property("version", version)

    // Note: Gradle does not analyze regexps below, so this variable tells Gradle
    // to treat the task input out of date when filtering logic is updated.
    inputs.property("replace.logic.version.bump.when.updating.filter.below", 1)

    outputs.dir(generatedVersionDir)

    into(generatedVersionDir)
    from("$projectDir/src/main/version") {
        include("**/*.java")
        val prop = Regex("""("[^"]++"|\S+)\s+/\* :(\w+) \*/""")
        filter { x: String ->
            prop.replace(x) {
                val variableName = it.groups[2]?.value
                when (variableName) {
                    "version" -> "\"$version\""
                    "major" -> major
                    "minor" -> minor
                    else -> "unknown variable: $x"
                } + """ /* :$variableName */"""
            }
        }
    }
}

ide {
    generatedJavaSources(versionClass.get(), generatedVersionDir.asFile)
}

sourceSets {
    main {
        resources.exclude("version/org-apache-calcite-jdbc.properties")
    }
}

tasks.withType<Checkstyle>().configureEach {
    exclude("org/apache/calcite/runtime/Resources.java")
}

val fmppMain by tasks.registering(org.apache.calcite.buildtools.fmpp.FmppTask::class) {
    config.set(file("src/main/codegen/config.fmpp"))
    templates.set(file("src/main/codegen/templates"))
}

val javaCCMain by tasks.registering(org.apache.calcite.buildtools.javacc.JavaCCTask::class) {
    dependsOn(fmppMain)
    val parserFile = fmppMain.map {
        it.output.asFileTree.matching { include("**/Parser.jj") }
    }
    inputFile.from(parserFile)
    packageName.set("org.apache.calcite.sql.parser.impl")
}

tasks.compileKotlin {
    dependsOn(versionClass)
    dependsOn(javaCCMain)
}

val fmppTest by tasks.registering(org.apache.calcite.buildtools.fmpp.FmppTask::class) {
    config.set(file("src/test/codegen/config.fmpp"))
    templates.set(file("src/main/codegen/templates"))
}

val javaCCTest by tasks.registering(org.apache.calcite.buildtools.javacc.JavaCCTask::class) {
    dependsOn(fmppTest)
    val parserFile = fmppTest.map {
        it.output.asFileTree.matching { include("**/Parser.jj") }
    }
    inputFile.from(parserFile)
    packageName.set("org.apache.calcite.sql.parser.parserextensiontesting")
}

tasks.compileTestKotlin {
    dependsOn(javaCCTest)
}

tasks.withType<Checkstyle>().configureEach {
    mustRunAfter(versionClass)
    mustRunAfter(javaCCMain)
    mustRunAfter(javaCCTest)
}

tasks.withType<AutostyleTask>().configureEach {
    mustRunAfter(versionClass)
    mustRunAfter(javaCCMain)
    mustRunAfter(javaCCTest)
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
    kotlinOptions {
        jvmTarget = "1.8"
    }
}
ide {
    fun generatedSource(javacc: TaskProvider<org.apache.calcite.buildtools.javacc.JavaCCTask>, sourceSet: String) =
        generatedJavaSources(javacc.get(), javacc.get().output.get().asFile, sourceSets.named(sourceSet))

    generatedSource(javaCCMain, "main")
    generatedSource(javaCCTest, "test")
}

fun JavaCompile.configureAnnotationSet(sourceSet: SourceSet) {
    source = sourceSet.java
    classpath = sourceSet.compileClasspath
    options.compilerArgs.add("-proc:only")
    org.gradle.api.plugins.internal.JvmPluginsHelper.configureAnnotationProcessorPath(sourceSet, sourceSet.java, options, project)
    destinationDirectory.set(temporaryDir)

    // only if we aren't running java compilation, since doing twice fails (in some places)
    onlyIf { !project.gradle.taskGraph.hasTask(sourceSet.getCompileTaskName("java")) }
}

val annotationProcessorMain by tasks.registering(JavaCompile::class) {
    configureAnnotationSet(sourceSets.main.get())
}

val annotationProcessorTest by tasks.registering(JavaCompile::class) {
    val kotlinTestCompile = tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>()
        .getByName("compileTestKotlin")

    dependsOn(javaCCTest, kotlinTestCompile)

    configureAnnotationSet(sourceSets.test.get())
    classpath += files(kotlinTestCompile.destinationDirectory.get())

    // only if we aren't running compileJavaTest, since doing twice fails.
    onlyIf { tasks.findByPath("compileTestJava")?.enabled != true }
}

ide {
    // generate annotation processed files on project import/sync.
    fun addSync(compile: TaskProvider<JavaCompile>) {
        project.rootProject.configure<org.gradle.plugins.ide.idea.model.IdeaModel> {
            project {
                settings {
                    taskTriggers {
                        afterSync(compile.get())
                    }
                }
            }
        }
    }

    addSync(annotationProcessorMain)
    addSync(annotationProcessorTest)
}

val integTestAll by tasks.registering() {
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    description = "Executes integration JDBC tests for all DBs"
}

fun capitalize(input: String): String {
    return input.replaceFirstChar { it.uppercaseChar() }
}

for (db in listOf("h2", "mysql", "oracle", "postgresql")) {
    val task = tasks.register("integTest" + capitalize(db), Test::class) {
        group = LifecycleBasePlugin.VERIFICATION_GROUP
        description = "Executes integration JDBC tests with $db database"
        include("org/apache/calcite/test/JdbcAdapterTest.class")
        include("org/apache/calcite/test/JdbcTest.class")
        systemProperty("calcite.test.db", db)
        // Include the jars from the custom configuration to the classpath
        // otherwise the JDBC drivers for each DBMS will be missing
        classpath += configurations.getAt("test" + capitalize(db))
    }
    integTestAll {
        dependsOn(task)
    }
}

// Task to run CoreQuidemTest with specific .iq file(s) from command line
tasks.register<JavaExec>("runQuidemTest") {
    group = "verification"
    description = "Run CoreQuidemTest with specific .iq file(s). Usage: ./gradlew :core:runQuidemTest -Pargs=\"sql/file.iq\""

    dependsOn(tasks.testClasses)
    classpath = sourceSets["test"].runtimeClasspath
    mainClass.set("org.apache.calcite.test.CoreQuidemTest")

    // Get arguments from -Pargs="file.iq" or -Pargs="file1.iq,file2.iq"
    if (project.hasProperty("args")) {
        args(project.property("args").toString().split(",").map { it.trim() })
    }

    // Forward JVM args for profiling
    if (System.getProperty("org.gradle.jvmargs") != null) {
        jvmArgs(System.getProperty("org.gradle.jvmargs").split(" "))
    }
}

// Task to run the MULTI query sharing benchmark
tasks.register<JavaExec>("runBenchmark") {
    group = "benchmark"
    description = "Run MULTI query sharing benchmark. Usage: ./gradlew :core:runBenchmark -PbenchmarkArgs=\"--queries=50\""

    dependsOn(tasks.testClasses)
    classpath = sourceSets["test"].runtimeClasspath
    mainClass.set("org.apache.calcite.test.MultiQueryBenchmarkCli")
    maxHeapSize = "2g"

    // Get arguments from -PbenchmarkArgs="--queries=50 --iterations=10"
    if (project.hasProperty("benchmarkArgs")) {
        args(project.property("benchmarkArgs").toString().split(" ").filter { it.isNotBlank() })
    }
}

// Task to run the WCOJ benchmark (baseline vs WCOJ vs multi-query WCOJ)
tasks.register<JavaExec>("runWcojBenchmark") {
    group = "benchmark"
    description = "Run WCOJ benchmark. Usage: ./gradlew :core:runWcojBenchmark -PwcojBenchmarkArgs=\"--nodes=200 --edges=1000\""

    dependsOn(tasks.testClasses)
    classpath = sourceSets["test"].runtimeClasspath
    mainClass.set("org.apache.calcite.test.WCOJBenchmarkCli")
    maxHeapSize = "2g"

    // WCOJ requires this system property to be enabled
    jvmArgs("-Dcalcite.enable.wcoj=true")

    if (project.hasProperty("wcojBenchmarkArgs")) {
        args(project.property("wcojBenchmarkArgs").toString().split(" ").filter { it.isNotBlank() })
    }
}

// Task to run the TPC-H WCOJ benchmark (cyclic joins on realistic data)
tasks.register<JavaExec>("runTpchWcojBenchmark") {
    group = "benchmark"
    description = "Run TPC-H WCOJ benchmark. Usage: ./gradlew :core:runTpchWcojBenchmark -PtpchWcojBenchmarkArgs=\"--scale=0.01\""

    dependsOn(tasks.testClasses)
    classpath = sourceSets["test"].runtimeClasspath
    mainClass.set("org.apache.calcite.test.TpchWCOJBenchmarkCli")
    maxHeapSize = "4g"

    // WCOJ requires this system property to be enabled
    jvmArgs("-Dcalcite.enable.wcoj=true")

    if (project.hasProperty("tpchWcojBenchmarkArgs")) {
        args(project.property("tpchWcojBenchmarkArgs").toString().split(" ").filter { it.isNotBlank() })
    }
}

// Quick TPC-H query exploration tool
tasks.register<JavaExec>("exploreTpchQueries") {
    group = "benchmark"
    dependsOn(tasks.testClasses)
    classpath = sourceSets["test"].runtimeClasspath
    mainClass.set("org.apache.calcite.test.TpchQueryExplorer")
    maxHeapSize = "4g"
    jvmArgs("-Dcalcite.enable.wcoj=true")
    if (project.hasProperty("exploreArgs")) {
        args(project.property("exploreArgs").toString().split(" ").filter { it.isNotBlank() })
    }
}
