FROM openjdk:11.0.12-jre

ENV DIRPATH /node

EXPOSE 6862 6864 6865

COPY src/docker/run_script.sh ${DIRPATH}/run_script.sh
RUN chmod +x ${DIRPATH}/run_script.sh

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libjemalloc-dev \
        python3 \
        python3-pip \
        python3-wheel && \
    apt-get remove -y libcurl4 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install hvac setuptools && \
    pip3 install pyhocon

COPY src/docker/launcher.py              ${DIRPATH}/launcher.py
COPY src/main/resources/application.conf ${DIRPATH}/application.conf
COPY ./target/waves-enterprise-all-*.jar ${DIRPATH}/we.jar

WORKDIR ${DIRPATH}

CMD [ "./run_script.sh" ]