# =============================================================================
# Message Generator — Dockerfile (task 3)
#
# Multi-stage build:
#   Stage 1 (maven:3.9-eclipse-temurin-17): compiles the service and packages
#          a runnable jar.
#   Stage 2 (eclipse-temurin:17-jre):       minimal JRE runtime image.
#
# Build context: this service folder (must contain pom.xml and src/).
# The packaged jar is normalized to /workspace/app.jar, so the pom does not
# need any specific finalName / packaging convention.
# =============================================================================

FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /workspace

# 1) Copy the pom first and warm the dependency cache: this layer is only
#    rebuilt when pom.xml changes, not on every source change.
#    (If resolution fails offline the final `mvn package` retries it anyway.)
COPY pom.xml ./
RUN mvn -B -q -DskipTests dependency:go-offline || true

# 2) Copy the sources and package. The "original-*.jar" backup produced by the
#    maven-shade-plugin (if used) is excluded; the first remaining jar becomes
#    the single artifact shipped to the runtime stage.
COPY src ./src
RUN mvn -B -q -DskipTests package \
 && JAR="$(find target -maxdepth 1 -name '*.jar' ! -name 'original-*' | sort | head -n 1)" \
 && [ -n "$JAR" ] \
 && cp "$JAR" app.jar

# --- Runtime stage -----------------------------------------------------------
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /workspace/app.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
