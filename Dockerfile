# Custom EMR Serverless runtime image for TradeLens. Must extend AWS's own
# EMR Serverless base image (arbitrary base images aren't supported) — this
# is the release label pinned in .env's TRADELENS_EMR_RELEASE_LABEL.
#
# This replaces the fragile --jars/--py-files/--files zip-shipping approach
# from scripts/submit_emr_serverless.sh's early revisions: EMR Serverless's
# runtime has no internet egress, so anything the job needs (Delta's JARs,
# the `delta`/`yaml`/`dotenv` Python packages, the app code itself) has to
# already be present. Baking them into the image at build time — which DOES
# have normal internet access, whether built locally or in CI — means the
# job submission itself needs none of that machinery anymore.
FROM public.ecr.aws/emr-serverless/spark/emr-7.2.0:latest

USER root

# Delta Lake JARs (JVM side). Same three local Ivy resolution needs — see
# common/spark_session.py — fetched at build time since the deployed runtime
# can't reach Maven Central itself (confirmed: spark.jars.packages timed out
# after ~9 minutes of retries in an earlier attempt).
RUN curl -fSL -o /usr/lib/spark/jars/delta-spark_2.12-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    curl -fSL -o /usr/lib/spark/jars/delta-storage-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar && \
    curl -fSL -o /usr/lib/spark/jars/antlr4-runtime-4.9.3.jar \
      https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar

# Python-side packages missing from the base image's runtime. NOT pandas —
# unlike these, pandas' compiled _libs extensions are core, not an optional
# accelerator, and the base image already ships one matched to its own
# Python/pyarrow build for pandas_udf support.
RUN pip3 install --no-cache-dir delta-spark==3.2.0 pyyaml python-dotenv

# Application code + config. PYTHONPATH addition mirrors the local
# PYTHONPATH=src convention (see Makefile) so `import tradelens` resolves.
COPY src/tradelens /home/hadoop/tradelens
COPY config/config.yaml /home/hadoop/config.yaml
ENV PYTHONPATH="/home/hadoop:${PYTHONPATH}"
ENV TRADELENS_CONFIG_PATH="/home/hadoop/config.yaml"
ENV TRADELENS_ENV="aws"

USER hadoop:hadoop
