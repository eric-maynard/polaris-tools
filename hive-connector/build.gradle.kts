plugins {
    java
    application
}

group = "org.apache.polaris"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

// Define versions for dependencies
val hadoopVersion by extra("3.4.1")
val hiveVersion by extra("3.1.3")
val slf4jVersion by extra("2.0.17")
val junitVersion by extra("5.12.2")

dependencies {
    // Hive Metastore client
    implementation("org.apache.hive:hive-metastore:${property("hiveVersion")}")
    implementation("org.apache.hive:hive-exec:${property("hiveVersion")}") // Often needed for Table objects and other utilities

    // Hadoop dependencies (Hive depends on Hadoop)
    implementation("org.apache.hadoop:hadoop-common:${property("hadoopVersion")}")
    implementation("org.apache.hadoop:hadoop-client:${property("hadoopVersion")}") // For FileSystem, Configuration, etc.

    // Logging: SLF4J API with a simple backend
    implementation("org.slf4j:slf4j-api:${property("slf4jVersion")}")
    runtimeOnly("org.slf4j:slf4j-simple:${property("slf4jVersion")}") // Or use Log4j2, Logback, etc.

    // Testing
    testImplementation(platform("org.junit:junit-bom:${property("junitVersion")}"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

application {
    // Optional: Define your main class if you're building a runnable application
    // mainClass.set("com.example.hive.MainApplication")
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8 // Or higher, e.g., 11, 17
    targetCompatibility = JavaVersion.VERSION_1_8
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
