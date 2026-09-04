"""Unit tests for quality/data_quality.py's precision_recall."""


def test_precision_recall_matches_hand_calc(spark):
    from tradelens.quality.data_quality import precision_recall

    # ground truth: O1, O2 are true spoofing; O3 is normal
    labels = spark.createDataFrame(
        [("O1", "abuse", "spoofing"), ("O2", "abuse", "spoofing"), ("O3", "normal", None)],
        ["order_id", "label", "pattern"],
    )
    # detector flagged O1 (true positive) and O3 (false positive); missed O2 (false negative)
    alerts = spark.createDataFrame([("O1", "spoofing"), ("O3", "spoofing")], ["order_id", "pattern"])

    result = precision_recall(alerts, labels, "spoofing")

    assert result["tp"] == 1
    assert result["fp"] == 1
    assert result["fn"] == 1
    assert result["precision"] == 0.5
    assert result["recall"] == 0.5


def test_precision_recall_different_alert_and_truth_pattern_names(spark):
    from tradelens.quality.data_quality import precision_recall

    labels = spark.createDataFrame([("O1", "abuse", "layering")], ["order_id", "label", "pattern"])
    alerts = spark.createDataFrame([("O1", "rapid_ordering")], ["order_id", "pattern"])

    result = precision_recall(alerts, labels, "rapid_ordering", truth_pattern="layering")

    assert result["tp"] == 1
    assert result["precision"] == 1.0
    assert result["recall"] == 1.0
