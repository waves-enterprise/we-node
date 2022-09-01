FROM openjdk:11.0.15-jre

ENV DIRPATH /node

EXPOSE 6862 6864 6865

COPY node/src/docker/run_script.sh ${DIRPATH}/run_script.sh
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

COPY node/src/docker/launcher.py              ${DIRPATH}/launcher.py
COPY node/src/main/resources/application.conf ${DIRPATH}/application.conf
COPY node/target/node-*.jar                   ${DIRPATH}/we.jar

WORKDIR ${DIRPATH}

CMD [ "./run_script.sh" ]
