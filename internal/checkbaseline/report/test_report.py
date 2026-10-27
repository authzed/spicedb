import pytest

import render


def test_bands_are_sample_quartiles():
    assert render.summarize([1, 2, 3, 4]) == dict(n=4, low=1, q1=1.75, median=2.5, q3=3.25, high=4)


def test_ratio_uses_pairs():
    assert render.paired_ratio([1, 100], [2, 100])["median"] == 1.5


def test_script_data_escapes_html():
    assert "</script>" not in render.safe_json({"name": "</script><script>alert(1)</script>"})


def test_nonfinite_rejected():
    with pytest.raises(ValueError):
        render.summarize([float("nan")])
