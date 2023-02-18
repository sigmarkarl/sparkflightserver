plugins {
    java
    application
    id("com.google.cloud.tools.jib") version "3.3.1"
}

group = "org.simmi"
version = "1.0"

jib {
    from {
        image = "public.ecr.aws/l8m2k1n1/netapp/spark:graalvm-22.3.1"
        if (project.hasProperty("REGISTRY_USER")) {
            auth {
                username = project.findProperty("REGISTRY_USER")?.toString()
                password = project.findProperty("REGISTRY_PASSWORD")?.toString()
            }
        }
    }
    to {
        image = project.findProperty("APPLICATION_REPOSITORY")?.toString() ?: "public.ecr.aws/l8m2k1n1/netapp/spark:flightserver-graalvm-22.3.1"
        //tags = [project.findProperty("APPLICATION_TAG")?.toString() ?: "1.0"]
        if (project.hasProperty("REGISTRY_USER")) {
            var reg_user = project.findProperty("REGISTRY_USER")?.toString()
            var reg_pass = project.findProperty("REGISTRY_PASSWORD")?.toString()
            System.err.println("hello2 " + reg_user + " " + reg_pass)
            auth {
                username = reg_user
                password = reg_pass
            }
        }
    }
    containerizingMode = "packaged"
    container {
        mainClass = "org.simmi.SparkFlightServer"
        ports = listOf("33333")
        environment = mapOf("JAVA_TOOL_OPTIONS" to "-Djdk.lang.processReaperUseDefaultStackSize=true --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED")
    }
}

repositories {
    mavenCentral()
}

application {
    mainClass.set("org.simmi.SparkFlightServer")
    applicationDefaultJvmArgs = listOf("--add-opens=java.base/java.io=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens=java.base/sun.security.action=ALL-UNNAMED")
}

dependencies {
    implementation("com.fasterxml.jackson:jackson-bom:2.14.2")
    implementation("org.apache.arrow:flight-core:11.0.0")
    implementation("org.apache.arrow:arrow-flight:11.0.0")
    implementation("org.apache.spark:spark-core_2.12:3.3.1")
    implementation("org.apache.spark:spark-sql_2.12:3.3.1")
    implementation("io.kubernetes:client-java:17.0.1")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.14.2")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.1")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}