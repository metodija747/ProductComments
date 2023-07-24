# Use AdoptOpenJDK's Docker image for OpenJDK 11 with Alpine Linux
FROM adoptopenjdk:11-jdk-hotspot

VOLUME /tmp
COPY target/master.microservice-comments-1.0.0.jar app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
