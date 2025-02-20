# Workshop
## How to Start the Service Locally

1. **Build the services**:
   ```bash
   mvn clean install -f ./datagenerator/pom.xml
   mvn clean install -f ./01-producer/pom.xml
   mvn clean install -f ./02-consumer/pom.xml
   mvn clean install -f ./03-avro/pom.xml
   mvn clean install -f ./04-stateless-streams/pom.xml
   mvn clean install -f ./05-stateful-streams-aggregate/pom.xml
   mvn clean install -f ./06-stateful-streams-window/pom.xml
   ```
2. **Start the services**:
   Use Docker Compose to start the application and its dependencies:
   ```bash
   docker-compose -f ./local-development/docker-compose.yml up --force-recreate --build --remove-orphans
   ```