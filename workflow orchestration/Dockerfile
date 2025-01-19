# First-time build can take upto 10 mins.

FROM apache/airflow:2.10.4

ENV AIRFLOW_HOME=/opt/airflow

# 使用root用户安装软件包和Google Cloud SDK
USER root
RUN apt-get update -qq && apt-get install vim -qqq



# 创建 dbt 项目目录
RUN mkdir -p /opt/dbt && \
    chown -R airflow:root /opt/dbt && \
    chmod -R g+rw /opt/dbt


# 使用更新的 Google Cloud SDK 版本
ARG CLOUD_SDK_VERSION=458.0.0
ENV GCLOUD_HOME=/opt/google-cloud-sdk
ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

WORKDIR $AIRFLOW_HOME

# 复制文件并设置权限时仍然使用root用户
COPY airflow/scripts scripts
RUN chmod +x scripts

COPY requirements.txt .

# 切换到airflow用户安装Python包
USER airflow
RUN pip install --no-cache-dir -r requirements.txt

# 安装 dbt-postgres,dbt-bigquery,dbt-core
RUN pip install --no-cache-dir \ 
        dbt-postgres==1.7.3 \
        dbt-bigquery==1.7.3 \
        dbt-core==1.7.3

# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

USER $AIRFLOW_UID