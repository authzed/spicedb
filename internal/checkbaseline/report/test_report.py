import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('baseline_report', Path(__file__).with_name('render.py'))
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)

class ReportTests(unittest.TestCase):
    def test_bands_are_sample_quartiles(self):
        s = report.summarize([1, 2, 3, 4])
        self.assertEqual(s, dict(n=4, low=1, q1=1.75, median=2.5, q3=3.25, high=4))
    def test_ratio_uses_pairs(self):
        s = report.paired_ratio([1, 100], [2, 100])
        self.assertEqual(s['median'], 1.5)
    def test_script_data_escapes_html(self):
        s = report.safe_json({'name':'</script><script>alert(1)</script>'})
        self.assertNotIn('</script>',s)
    def test_nonfinite_rejected(self):
        with self.assertRaises(ValueError): report.summarize([float('nan')])

if __name__ == '__main__': unittest.main()
