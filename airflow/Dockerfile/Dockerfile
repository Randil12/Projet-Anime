ARG AIRFLOW_IMAGE_TAG

FROM apache/airflow:${AIRFLOW_IMAGE_TAG}

USER root

# Installer Java (Requis pour la commande spark-submit locale)
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow