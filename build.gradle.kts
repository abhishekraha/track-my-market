plugins {
    java
    application
}

group = "com.trackmymarket"
version = "1.0.0-SNAPSHOT"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(25))
    }
}

val sbeGenerationDir = layout.buildDirectory.dir("generated/sources/sbe/main/java")

sourceSets {
    main {
        java {
            srcDir(sbeGenerationDir)
        }
    }
}

repositories {
    mavenCentral()
}

val sbeConfiguration: Configuration = configurations.create("sbeConfiguration")

dependencies {
    val aeronVersion = "1.46.7"
    val sbeVersion = "1.33.0"

    implementation("io.aeron:aeron-all:$aeronVersion")
    implementation("io.aeron:aeron-archive:$aeronVersion")
    implementation("uk.co.real-logic:sbe-all:$sbeVersion")

    sbeConfiguration("uk.co.real-logic:sbe-all:$sbeVersion")

    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testImplementation("org.assertj:assertj-core:3.26.3")
    testImplementation("org.mockito:mockito-core:5.14.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

val generateSbe = tasks.register<JavaExec>("generateSbe") {
    description = "Generates SBE Java stubs from schema"
    group = "build"
    classpath = sbeConfiguration
    mainClass.set("uk.co.real_logic.sbe.SbeTool")
    systemProperties(
        "sbe.output.dir" to sbeGenerationDir.get().asFile.absolutePath,
        "sbe.target.language" to "Java",
        "sbe.java.generate.interfaces" to "true"
    )
    args("src/main/resources/market-data-schema.xml")
    inputs.file("src/main/resources/market-data-schema.xml")
    outputs.dir(sbeGenerationDir)
}

tasks.named("compileJava") {
    dependsOn(generateSbe)
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED")
    testLogging {
        events("passed", "skipped", "failed", "standardOut", "standardError")
        showExceptions = true
        showCauses = true
        showStackTraces = true
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

application {
    mainClass.set("market.MarketApp")
}
