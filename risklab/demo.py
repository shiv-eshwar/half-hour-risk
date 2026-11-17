"""Load the published MLlib pipeline and independently regenerate two held-out warnings."""

import json
import math
import zipfile
from contextlib import contextmanager
from tempfile import TemporaryDirectory

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

from .core import RESULTS


def package_model(source):
    with zipfile.ZipFile(RESULTS / "selected-model.zip", "w", zipfile.ZIP_DEFLATED) as archive:
        for file in sorted(source.rglob("*")):
            if file.is_file() and not file.name.startswith("."):
                archive.write(file, file.relative_to(source))


@contextmanager
def released_model():
    with TemporaryDirectory() as folder:
        with zipfile.ZipFile(RESULTS / "selected-model.zip") as archive:
            archive.extractall(folder)
        yield PipelineModel.load(folder)


def demo(spark):
    cases = json.loads((RESULTS / "case_studies.json").read_text())
    rows = [{"case_id": k, **v["feature_values"]} for k, v in cases.items()]
    with released_model() as model:
        data = spark.createDataFrame(rows)
        scored = model.transform(data).select("case_id", vector_to_array("probability")[1].alias("score"))
        for prediction in scored.orderBy("case_id").collect():
            case = cases[prediction.case_id]
            assert math.isclose(prediction.score, case["score"], rel_tol=1e-10, abs_tol=1e-12)
            print(f"{prediction.case_id}  {case['issue_utc']}  score {prediction.score:.4f}  "
                  f"cutoff {case['alert_cutoff']:.4f}  alert {prediction.score > case['alert_cutoff']}  "
                  f"observed stress {bool(case['observed_event'])}")
