#!/usr/bin/env bash
# Package src/ + config and sync to the code bucket for EMR Serverless.
set -euo pipefail
source .env
export AWS_PROFILE="${AWS_PROFILE:?Set AWS_PROFILE in .env}"
# Need the project venv's `delta` package importable (see below) — plain
# `python` on PATH may resolve to an unrelated interpreter otherwise.
source .venv/Scripts/activate

echo ">> Zipping source"
rm -f tradelens_src.zip
python -c "
import pathlib, zipfile
src = pathlib.Path('src')
with zipfile.ZipFile('tradelens_src.zip', 'w', zipfile.ZIP_DEFLATED) as zf:
    for f in src.rglob('*.py'):
        zf.write(f, f.relative_to(src).as_posix())
"

# EMR Serverless has no internet egress and its Spark runtime's Python env
# only has what it ships with — run_pipeline.py's non-pyspark, non-stdlib
# imports (delta.tables, yaml, dotenv) aren't in it. Ship pure-Python-only
# copies of each pulled from this venv: delta.tables.DeltaTable's JVM side is
# separately covered by --jars in submit_emr_serverless.sh; yaml ships
# WITHOUT its compiled _yaml*.pyd (Windows-only — PyYAML falls back to its
# pure-Python loader when that accelerator is absent, and common/config.py
# only calls yaml.safe_load, so the fallback is all it needs); dotenv has no
# compiled parts at all. pandas is NOT shipped here — it's core-C-extension
# throughout (not an optional accelerator like PyYAML's), so a Windows build
# of it cannot run on EMR's Linux workers at all; EMR's Spark runtime already
# bundles a matching pandas for pandas_udf support, which is all
# gold/surveillance_alerts.py needs.
echo ">> Zipping delta/yaml/dotenv python packages (for --py-files; delta's JVM side is via --jars)"
rm -f python_deps.zip
python -c "
import pathlib, zipfile, delta, yaml, dotenv

def add_pkg(zf, module, exclude_suffixes=()):
    src = pathlib.Path(module.__file__).parent
    name = src.name
    for f in src.rglob('*'):
        if f.is_file() and f.suffix not in ('.pyc',) + exclude_suffixes:
            zf.write(f, name + '/' + f.relative_to(src).as_posix())

with zipfile.ZipFile('python_deps.zip', 'w', zipfile.ZIP_DEFLATED) as zf:
    add_pkg(zf, delta)
    add_pkg(zf, yaml, exclude_suffixes=('.pyd', '.so'))
    add_pkg(zf, dotenv)
"

echo ">> Uploading to s3://${TRADELENS_CODE_BUCKET}/"
aws s3 cp tradelens_src.zip "s3://${TRADELENS_CODE_BUCKET}/code/tradelens_src.zip"
aws s3 cp python_deps.zip "s3://${TRADELENS_CODE_BUCKET}/code/python_deps.zip"
aws s3 cp src/tradelens/jobs/run_pipeline.py "s3://${TRADELENS_CODE_BUCKET}/code/run_pipeline.py"
aws s3 cp config/config.yaml "s3://${TRADELENS_CODE_BUCKET}/code/config.yaml"
echo ">> Done."
