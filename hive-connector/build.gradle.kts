plugins {
    java
    application
}

group = "org.apache.polaris"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven {
        name = "Apache Snapshots"
        url = uri("https://repository.apache.org/content/repositories/snapshots/")
    }
}

// Define versions for dependencies
val polarisVersion by extra("1.1.0-incubating-SNAPSHOT")
val icebergVersion by extra("1.9.0")
val hadoopVersion by extra("3.4.1")
val hiveVersion by extra("3.1.3")
val slf4jVersion by extra("2.0.17")
val junitVersion by extra("5.12.2")

dependencies {
    // Polaris dep
    implementation("org.apache.polaris:polaris-service-common:${property("polarisVersion")}")
    implementation("org.apache.polaris:polaris-core:${property("polarisVersion")}")
    implementation("org.apache.iceberg:iceberg-api:${property("icebergVersion")}")
    implementation("org.apache.iceberg:iceberg-core:${property("icebergVersion")}")

    // Hive Metastore client
    implementation("org.apache.hive:hive-metastore:${property("hiveVersion")}")
    implementation("org.apache.hive:hive-exec:${property("hiveVersion")}")

    // Hadoop dependencies (Hive depends on Hadoop)
    implementation("org.apache.hadoop:hadoop-common:${property("hadoopVersion")}")
    implementation("org.apache.hadoop:hadoop-client:${property("hadoopVersion")}")

    // Commons
    implementation("org.apache.commons:commons-pool2:2.11.1")

    // Logging: SLF4J API with a simple backend
    implementation("org.slf4j:slf4j-api:${property("slf4jVersion")}")
    runtimeOnly("org.slf4j:slf4j-simple:${property("slf4jVersion")}")

    // Testing
    testImplementation(platform("org.junit:junit-bom:${property("junitVersion")}"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

tasks.test {
    useJUnitPlatform()
}

// To handle potential dependency conflicts, especially with Hadoop/Hive
configurations.all {
    resolutionStrategy {
        // Example: Force a specific version of a transitive dependency
        // force("com.google.guava:guava:23.0")

        // Exclude conflicting modules
        // exclude(group = "org.slf4j", module = "slf4j-log4j12")
        // exclude(group = "log4j", module = "log4j")
    }
}
