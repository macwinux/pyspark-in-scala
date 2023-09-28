FROM bitnami/spark:3.1.2

USER root
ARG SBT_VERSION=1.6.2
ARG TANGO_HOST=localhost:10000
# Install custom sbt if needed
# See metals default sbt limitations: https://scalameta.org/metals/docs/editors/vscode.html#custom-sbt-launcher

WORKDIR /workspaces

RUN \
  mkdir /working/ && \
  cd /working/ && \
  curl -L -o sbt-$SBT_VERSION.deb https://repo.scala-sbt.org/scalasbt/debian/sbt-$SBT_VERSION.deb && \
  dpkg -i sbt-$SBT_VERSION.deb && \
  rm sbt-$SBT_VERSION.deb && \
  apt-get update && \
  apt-get install sbt && \
  cd && \
  rm -r /working/
RUN echo "tango-common tango-common/tango-host string $TANGO_HOST" | debconf-set-selections
RUN apt-get install -y --no-install-recommends python3.6
RUN apt-get install -y python3-pip
RUN pip install pyspark==3.1.2
RUN pip install pyarrow
RUN pip install pandas

#
# Installing package into Spark if needed
# spark-shell --master local --packages "<package name>"

