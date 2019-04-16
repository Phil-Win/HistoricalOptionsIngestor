FROM openjdk:8-jdk-alpine
MAINTAINER himynameisfil@gmail.com
COPY build/libs/*.jar HistoricalOptionsIngestor.jar
ENTRYPOINT ["java","-jar","HistoricalOptionsIngestor.jar", "bootRun"]
