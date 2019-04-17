FROM openjdk:8-jdk-alpine
MAINTAINER himynameisfil@gmail.com
RUN mkdir -p /data/historical_options_input
RUN mkdir -p /config
COPY build/libs/*.jar HistoricalOptionsIngestor.jar
ENTRYPOINT ["java","-jar","HistoricalOptionsIngestor.jar", "bootRun"]
