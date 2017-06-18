FROM saifu/python-base

############################################################
# Container environment variables
############################################################
ENV BASE_APP_DIR=/usr/local/bin/saifu
ENV CORE_BASE_DIR=${BASE_APP_DIR}/saifu
ENV CORE_DIR=${CORE_BASE_DIR}/core
ENV PYTHONPATH $PYTHONPATH:${BASE_APP_DIR}
############################################################
# Initializes the base image for the core components.
############################################################

RUN mkdir -p ${CORE_DIR}
RUN touch ${CORE_BASE_DIR}/__init__.py
COPY . ${CORE_DIR}
WORKDIR ${CORE_DIR}
RUN pip install --no-cache-dir -r requirements.txt

############################################################
# Initializes the child container
############################################################
WORKDIR ${BASE_APP_DIR}
ONBUILD COPY requirements.txt ${BASE_APP_DIR}
ONBUILD RUN pip install --no-cache-dir -r requirements.txt
ONBUILD COPY . ${BASE_APP_DIR}
