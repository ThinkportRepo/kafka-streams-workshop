# Workshop
## How to Start the Service Locally

1. **Build the services**:
   ```bash
   mvn clean install -f ./datagenerator/pom.xml
   mvn clean install -f ./01-producer/pom.xml
   mvn clean install -f ./02-consumer/pom.xml
   ```
2. **Start the services**:
   Use Docker Compose to start the application and its dependencies:
   ```bash
   docker-compose -f ./local-development/docker-compose.yml up --force-recreate --build --remove-orphans
   ```