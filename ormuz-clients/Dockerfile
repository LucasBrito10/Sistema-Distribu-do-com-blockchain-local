FROM maven:3.9.5-eclipse-temurin-21 AS build
WORKDIR /app

# Copia o pom mestre e todos os módulos
COPY pom.xml .
COPY ormuz-shared  ./ormuz-shared
COPY ormuz-broker  ./ormuz-broker
COPY ormuz-clients ./ormuz-clients

RUN mvn clean package -pl ormuz-clients -am -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app

COPY --from=build /app/ormuz-clients/target/ormuz-clients-1.0-SNAPSHOT.jar clients.jar

ENV APP_CLASS="com.ormuz.client.app.AppFrotaDrones"
ENV BROKER_IP="127.0.0.1"
ENV BROKER_PORT="8080"
ENV SECTOR="SETOR_NORTE"

ENTRYPOINT ["sh", "-c", "exec java -cp clients.jar \"$APP_CLASS\" \"$BROKER_IP\" \"$BROKER_PORT\" \"$SECTOR\""]