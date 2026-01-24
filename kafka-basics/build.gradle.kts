plugins {
    id("java")
}

group = "com.bwardweb"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Source: https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
    implementation("org.apache.kafka:kafka-clients:4.1.0")

    // Source: https://mvnrepository.com/artifact/org.slf4j/slf4j-api
    implementation("org.slf4j:slf4j-api:2.0.17")

    // Source: https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
    implementation("org.slf4j:slf4j-simple:2.0.17")

}

tasks.test {
    useJUnitPlatform()
}