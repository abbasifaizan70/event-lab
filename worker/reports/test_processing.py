import unittest
from reports.processing import make_result

class ProcessingTests(unittest.TestCase):
    def test_whitespace_and_unicode(self):
        result = make_result({"type": "ReportRequested", "version": 1,
                              "reportId": "example", "text": "Hello  دنیا\nKafka"})
        self.assertEqual(result["wordCount"], 3)
        self.assertEqual(result["characterCount"], 17)
        self.assertEqual(result["reportId"], "example")

    def test_unsupported_contract_is_rejected(self):
        with self.assertRaises(ValueError):
            make_result({"type": "ReportRequested", "version": 2})

    def test_empty_text_is_rejected(self):
        with self.assertRaises(ValueError):
            make_result({"type": "ReportRequested", "version": 1, "text": "  "})
