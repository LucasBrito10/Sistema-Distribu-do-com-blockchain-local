FROM maven:3.9.5-eclipse-temurin-21 AS build
WORKDIR /app

# Copia o pom mestre e todos os módulos para satisfazer a árvore do Maven
COPY pom.xml .
COPY ormuz-shared  ./ormuz-shared
COPY ormuz-broker  ./ormuz-broker
COPY ormuz-clients ./ormuz-clients

RUN mvn clean package -pl ormuz-broker -am -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app

COPY --from=build /app/ormuz-broker/target/ormuz-broker-1.0-SNAPSHOT.jar broker.jar

ENV PORT=8080
ENV CLUSTER_IPS="127.0.0.1"
ENV LEDGER_FILE="/data/ledger.log"
RUN mkdir -p /data

EXPOSE 8080
EXPOSE 5701

ENTRYPOINT ["sh", "-c", "exec java -jar broker.jar \"$PORT\" \"$CLUSTER_IPS\""]