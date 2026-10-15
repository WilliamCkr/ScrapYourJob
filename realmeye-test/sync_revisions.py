#!/usr/bin/env python3
"""Collect RealmEye recent wiki changes into a deduplicated CSV.

Runs unattended in GitHub Actions. Does not access Google Sheets.
"""
import csv
import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen
from urllib.robotparser import RobotFileParser

BASE = "https://www.realmeye.com"
SOURCE = BASE + "/recent-changes-of-the-rotmg-wiki"
AGENT = "RealmEyeWikiChangesCSV/1.0"
HEADERS = ["Page", "Revision", "Comment", "Diff"]
OUTPUT = Path(__file__).parent / "data" / "revisions.csv"


def clean(value):
    return " ".join(str(value or "").split())


class Tables(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tables = []
        self.depth = 0
        self.table = None
        self.row = None
        self.cell = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "table":
            self.depth += 1
            if self.depth == 1:
                self.table = []
            return
        if self.depth != 1:
            return
        if tag == "tr":
            self.row = []
        elif tag in ("th", "td") and self.row is not None:
            self.cell = {"text": [], "abbr_title": None, "links": [], "tag": tag}
        elif self.cell is not None and tag == "abbr" and attrs.get("title") is not None:
            self.cell["abbr_title"] = attrs["title"]
        elif self.cell is not None and tag == "a" and attrs.get("href"):
            self.cell["links"].append(attrs["href"])

    def handle_data(self, value):
        if self.cell is not None:
            self.cell["text"].append(value)

    def handle_endtag(self, tag):
        if tag == "table":
            if self.depth == 1 and self.table is not None:
                self.tables.append(self.table)
                self.table = None
            self.depth = max(0, self.depth - 1)
            return
        if self.depth != 1:
            return
        if tag in ("td", "th") and self.cell is not None and self.row is not None:
            self.cell["text"] = clean("".join(self.cell["text"]))
            self.row.append(self.cell)
            self.cell = None
        elif tag == "tr" and self.row is not None and self.table is not None:
            if self.row:
                self.table.append(self.row)
            self.row = None


def get(url, max_bytes=3_000_000):
    request = Request(url, headers={
        "User-Agent": AGENT,
        "Accept": "text/html, text/plain;q=0.8, */*;q=0.5",
        "Accept-Encoding": "identity",
    })
    with urlopen(request, timeout=35) as response:
        payload = response.read(max_bytes + 1)
        if len(payload) > max_bytes:
            raise ValueError("Response too large to validate safely")
        if response.status != 200:
            raise ValueError("Unexpected HTTP status: " + str(response.status))
        return payload.decode(response.headers.get_content_charset() or "utf-8", "replace")


def download_source():
    robots = RobotFileParser()
    robots.parse(get(BASE + "/robots.txt").splitlines())
    if not robots.can_fetch(AGENT, SOURCE):
        raise RuntimeError("RealmEye robots.txt disallows this path for the collector")
    return get(SOURCE)


def table_rows(html):
    parser = Tables()
    parser.feed(html)
    candidates = []
    for table in parser.tables:
        if not table:
            continue
        head = [clean(cell["text"]).lower() for cell in table[0]]
        if len(head) >= 3 and head[0] == "page" and head[1].startswith("rev") and head[2] == "comment":
            candidates.append(table)
    if len(candidates) != 1:
        raise ValueError("Expected exactly one recent-changes table, got " + str(len(candidates)))
    table = candidates[0]
    if len(table) < 11:
        raise ValueError("Too few table rows to confirm complete extraction: " + str(len(table)))

    revisions = []
    seen = set()
    expanded_comments = 0
    without_diff = 0
    for idx, cells in enumerate(table[1:], start=2):
        if len(cells) < 3:
            raise ValueError("Incomplete HTML row " + str(idx))
        page = clean(cells[0]["text"])
        revision_text = clean(cells[1]["text"])
        matched = re.match(r"^([0-9]+)", revision_text)
        if not page or not matched:
            raise ValueError("Invalid page or revision at HTML row " + str(idx))
        revision = str(int(matched.group(1)))
        visible_comment = clean(cells[2]["text"])
        complete = cells[2]["abbr_title"]
        comment = clean(complete) if complete is not None else visible_comment
        if complete is not None and comment != visible_comment:
            expanded_comments += 1
        if complete is None and "..." in visible_comment:
            raise ValueError("Possible truncated comment at HTML row " + str(idx))
        compare = [
            urljoin(BASE, href)
            for href in cells[1]["links"]
            if "/compare-revisions-of-wiki-page/" in href
        ]
        if len(compare) > 1:
            raise ValueError("Ambiguous diff URL at HTML row " + str(idx))
        diff = compare[0] if compare else ""
        if diff:
            parsed = urlparse(diff)
            if parsed.scheme != "https" or parsed.netloc != "www.realmeye.com":
                raise ValueError("Invalid diff URL in HTML row " + str(idx))
            if not parsed.path.rstrip("/").endswith("/" + revision):
                raise ValueError("Diff revision number mismatch at HTML row " + str(idx))
        else:
            without_diff += 1
        key = (page.casefold(), revision)
        if key in seen:
            raise ValueError("Duplicate row on RealmEye: " + str(key))
        seen.add(key)
        revisions.append({"Page": page, "Revision": revision, "Comment": comment, "Diff": diff})
    print("Live source rows:", len(revisions))
    print("Comments expanded from HTML title:", expanded_comments)
    print("Rows without compare link:", without_diff)
    print("First parsed row:", revisions[0])
    return revisions


def load_existing():
    if not OUTPUT.exists():
        return []
    with OUTPUT.open("r", newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != HEADERS:
            raise ValueError("CSV headers changed; refusing to overwrite historical data")
        rows = list(reader)
    if any(not r["Page"] or not r["Revision"] for r in rows):
        raise ValueError("Existing CSV contains invalid revision keys")
    return rows


def main():
    incoming = table_rows(download_source())
    existing = load_existing()
    seen = {(row["Page"].casefold(), str(row["Revision"])) for row in existing}
    if len(seen) != len(existing):
        raise ValueError("Existing CSV has duplicates; refusing write")
    additions = [
        row for row in reversed(incoming)
        if (row["Page"].casefold(), row["Revision"]) not in seen
    ]
    if not additions:
        print("No new revisions. CSV unchanged; existing rows:", len(existing))
        return
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    temp = OUTPUT.with_suffix(".csv.tmp")
    with temp.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=HEADERS, lineterminator=chr(10))
        writer.writeheader()
        writer.writerows(existing + additions)
    temp.replace(OUTPUT)
    print("New revisions:", len(additions), "; total:", len(existing) + len(additions))
    print("Saved CSV:", OUTPUT)


if __name__ == "__main__":
    main()
