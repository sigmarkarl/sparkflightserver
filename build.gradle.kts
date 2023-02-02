plugins {
    id("java")
}

group = "org.simmi"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.fasterxml.jackson:jackson-bom:2.14.1")
    implementation("org.apache.arrow:flight-core:10.0.1")
    implementation("org.apache.arrow:arrow-flight:10.0.1")
    implementation("org.apache.spark:spark-core_2.12:3.3.1")
    implementation("org.apache.spark:spark-sql_2.12:3.3.1")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.1")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}