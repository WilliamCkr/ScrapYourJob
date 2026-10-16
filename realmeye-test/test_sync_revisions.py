"""Regression tests: zero changes, one new revision, deduplication, and large history."""

import csv
import importlib.util
import tempfile
import unittest
from html import escape
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parent / "sync_revisions.py"
spec = importlib.util.spec_from_file_location("sync_revisions", SCRIPT)
sync = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sync)


def html_table(revisions):
    rows = ["<table><tr><th>Page</th><th>Rev.</th><th>Comment</th></tr>"]
    for page, revision, comment in revisions:
        page_html = escape(page)
        comment_html = escape(comment)
        diff_html = (
            '<a title="Diff with the previous revision" '
            f'href="/compare-revisions-of-wiki-page/sample/{revision-1}/{revision}">P</a>'
            if revision > 1 else ""
        )
        rows.append(
            f'<tr><td>{page_html}</td><td>{revision}{diff_html}</td>'
            f'<td><abbr title="{escape(comment, quote=True)}">'
            f'{comment_html[:5]}...</abbr></td></tr>'
        )
    rows.append("</table>")
    return "".join(rows)


def read_csv(path):
    with path.open("r", newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def write_csv(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=sync.HEADERS)
        writer.writeheader()
        writer.writerows(data)


class RealmEyeSyncTests(unittest.TestCase):
    def run_sync(self, output, revisions):
        with patch.object(sync, "OUTPUT", output):
            with patch.object(sync, "download_source", return_value=html_table(revisions)):
                sync.main()

    def test_zero_revision_rows_leave_existing_csv_unchanged(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "revisions.csv"
            write_csv(path, [{"Page": "Existing", "Revision": "3", "Comment": "saved", "Diff": ""}])
            before = path.read_bytes()
            self.run_sync(path, [])
            self.assertEqual(path.read_bytes(), before)

    def test_single_new_revision_is_appended_immediately(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "revisions.csv"
            write_csv(path, [{"Page": "Existing", "Revision": "3", "Comment": "saved", "Diff": ""}])
            self.run_sync(path, [("Brand New", 7, "full comment text")])
            rows = read_csv(path)
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["Page"], "Existing")
            self.assertEqual(rows[1]["Page"], "Brand New")
            self.assertEqual(rows[1]["Revision"], "7")
            self.assertEqual(rows[1]["Comment"], "full comment text")
            self.assertEqual(
                rows[1]["Diff"],
                "https://www.realmeye.com/compare-revisions-of-wiki-page/sample/6/7",
            )

    def test_repeat_run_never_duplicates_a_single_revision(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "revisions.csv"
            items = [("Test Page", 9, "latest comment")]
            self.run_sync(path, items)
            before = path.read_bytes()
            self.run_sync(path, items)
            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(len(read_csv(path)), 1)

    def test_history_has_no_record_count_limit(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "revisions.csv"
            saved = [
                {"Page": f"Page {i}", "Revision": str(i + 1), "Comment": "saved", "Diff": ""}
                for i in range(1200)
            ]
            write_csv(path, saved)
            self.run_sync(path, [("New Page", 2, "new revision")])
            rows = read_csv(path)
            self.assertEqual(len(rows), 1201)
            self.assertEqual(rows[-1]["Page"], "New Page")


if __name__ == "__main__":
    unittest.main()
