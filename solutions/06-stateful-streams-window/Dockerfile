FROM eclipse-temurin:21-jre-alpine
COPY target/*.jar app.jar
EXPOSE 8079
EXPOSE 8078
ENTRYPOINT ["java","-jar","/app.jar"]