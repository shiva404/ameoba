"""Tests for the classification pipeline and individual layers."""

from __future__ import annotations

import pytest

from ameoba.domain.record import DataCategory, DataRecord
from ameoba.kernel.classifier.layers.binary import BinaryBlobDetector
from ameoba.kernel.classifier.layers.format import FormatDetector
from ameoba.kernel.classifier.layers.structural import StructuralAnalyser
from ameoba.kernel.classifier.pipeline import ClassificationPipeline


# ---------------------------------------------------------------------------
# Layer 0: Binary
# ---------------------------------------------------------------------------

class TestBinaryLayer:
    def test_jpeg_magic_bytes(self):
        detector = BinaryBlobDetector()
        jpeg = b"\xff\xd8\xff" + b"\x00" * 100
        ctx: dict = {}
        result = detector.classify(jpeg, ctx)
        assert result is not None
        assert result.primary_category == DataCategory.BLOB
        assert result.confidence > 0.9

    def test_pdf_magic(self):
        detector = BinaryBlobDetector()
        ctx: dict = {}
        result = detector.classify(b"%PDF-1.4 ...", ctx)
        assert result is not None
        assert result.blob > 0.9

    def test_high_entropy_bytes(self):
        import os
        detector = BinaryBlobDetector(entropy_threshold=7.0)
        random_bytes = os.urandom(10000)
        ctx: dict = {}
        result = detector.classify(random_bytes, ctx)
        assert result is not None
        assert result.blob > 0.8

    def test_plain_text_passes_through(self):
        detector = BinaryBlobDetector()
        ctx: dict = {}
        result = detector.classify(b"hello world", ctx)
        assert result is None  # Not blob — let next layer decide

    def test_binary_content_type(self):
        detector = BinaryBlobDetector()
        ctx = {"content_type": "image/png"}
        result = detector.classify(b"anything", ctx)
        assert result is not None
        assert result.blob > 0.9

    def test_dict_is_not_blob(self):
        detector = BinaryBlobDetector()
        ctx: dict = {}
        result = detector.classify({"key": "value"}, ctx)
        assert result is None


# ---------------------------------------------------------------------------
# Layer 1: Format
# ---------------------------------------------------------------------------

class TestFormatLayer:
    def test_json_dict_decoded(self):
        detector = FormatDetector()
        ctx: dict = {}
        result = detector.classify('{"name": "alice", "age": 30}', ctx)
        # JSON → let structural layer decide
        assert result is None
        assert ctx["decoded"] == {"name": "alice", "age": 30}
        assert ctx["format"] == "json"

    def test_csv_detected(self):
        detector = FormatDetector()
        ctx: dict = {}
        csv_data = "name,age,email\nalice,30,a@b.com\nbob,25,b@b.com\n"
        result = detector.classify(csv_data, ctx)
        assert result is not None
        assert result.relational > 0.7

    def test_xml_is_document(self):
        detector = FormatDetector()
        ctx: dict = {}
        result = detector.classify("<root><item>1</item></root>", ctx)
        assert result is not None
        assert result.document > 0.7

    def test_already_parsed_dict(self):
        detector = FormatDetector()
        data = {"key": "value"}
        ctx: dict = {}
        result = detector.classify(data, ctx)
        assert result is None  # Passes through
        assert ctx["decoded"] == data


# ---------------------------------------------------------------------------
# Layer 2: Structural
# ---------------------------------------------------------------------------

class TestStructuralLayer:
    def test_flat_consistent_dicts_are_relational(self):
        analyser = StructuralAnalyser()
        records = [
            {"id": 1, "name": "alice", "score": 9.5},
            {"id": 2, "name": "bob", "score": 8.0},
            {"id": 3, "name": "carol", "score": 7.5},
        ]
        ctx = {"decoded": records}
        result = analyser.classify(records, ctx)
        assert result is not None
        assert result.relational > result.document
        assert result.primary_category == DataCategory.RELATIONAL

    def test_nested_heterogeneous_is_document(self):
        analyser = StructuralAnalyser()
        records = [
            {"id": 1, "meta": {"deep": {"value": 1}}, "tags": [1, 2, 3]},
            {"id": 2, "meta": {"other": "field"}, "notes": "different"},
            {"id": 3, "extra_field": True},  # schema variance
        ]
        ctx = {"decoded": records}
        result = analyser.classify(records, ctx)
        assert result is not None
        assert result.document > result.relational

    def test_graph_structure(self):
        analyser = StructuralAnalyser()
        graph_data = {
            "nodes": [{"id": 1, "label": "A"}, {"id": 2, "label": "B"}],
            "edges": [{"source": 1, "target": 2}],
        }
        ctx = {"decoded": graph_data}
        result = analyser.classify(graph_data, ctx)
        assert result is not None
        assert result.graph > 0.0


# ---------------------------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------------------------

class TestClassificationPipeline:
    def _make_record(self, payload, collection="test", **kwargs) -> DataRecord:
        return DataRecord(collection=collection, payload=payload, **kwargs)

    def test_relational_data(self):
        pipeline = ClassificationPipeline()
        records = [{"id": i, "value": i * 2, "label": f"item-{i}"} for i in range(10)]
        record = self._make_record(records)
        result = pipeline.classify(record)
        assert result.primary_category == DataCategory.RELATIONAL

    def test_blob_bytes(self):
        import os
        pipeline = ClassificationPipeline()
        record = self._make_record(os.urandom(5000))
        result = pipeline.classify(record)
        assert result.primary_category == DataCategory.BLOB

    def test_category_hint_bypasses_pipeline(self):
        pipeline = ClassificationPipeline()
        record = self._make_record(
            {"messy": "data"},
            category_hint=DataCategory.GRAPH,
        )
        result = pipeline.classify(record)
        assert result.primary_category == DataCategory.GRAPH
        assert result.confidence == 1.0

    def test_document_data(self):
        pipeline = ClassificationPipeline()
        doc = {
            "title": "Report",
            "sections": [
                {"heading": "Intro", "content": "...", "footnotes": [1, 2]},
                {"heading": "Analysis", "content": "...", "refs": {"a": 1, "b": 2}},
            ],
            "author": {"name": "Bob", "dept": "research"},
        }
        record = self._make_record(doc)
        result = pipeline.classify(record)
        assert result.primary_category == DataCategory.DOCUMENT

    def test_vector_data(self):
        pipeline = ClassificationPipeline()
        record = self._make_record({
            "id": "vec-1",
            "embedding": [0.1, 0.2, 0.3] * 128,  # 384-dim
        })
        result = pipeline.classify(record)
        assert result.primary_category == DataCategory.VECTOR
