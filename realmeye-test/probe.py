#!/usr/bin/env python3
"""Read-only access test for RealmEye from a GitHub Actions runner."""
from html.parser import HTMLParser
from urllib.request import Request, urlopen
from urllib.robotparser import RobotFileParser
from urllib.error import HTTPError, URLError

ROOT = "https://www.realmeye.com"
PAGE = ROOT + "/recent-changes-of-the-rotmg-wiki"
AGENT = "RealmEye-Changes-Check/1.0"

class TableReader(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tables = []
        self.current_table = None
        self.current_row = None
        self.current_cell = None

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self.current_table = []
        elif tag == "tr" and self.current_table is not None:
            self.current_row = []
        elif tag in ("td", "th") and self.current_row is not None:
            self.current_cell = []
    def handle_data(self, data):
        if self.current_cell is not None:
            self.current_cell.append(data)
    def handle_endtag(self, tag):
        if tag in ("td", "th") and self.current_cell is not None:
            self.current_row.append(" ".join("".join(self.current_cell).split()))
            self.current_cell = None
        elif tag == "tr" and self.current_row is not None:
            self.current_table.append(self.current_row)
            self.current_row = None
        elif tag == "table" and self.current_table is not None:
            self.tables.append(self.current_table)
            self.current_table = None

def fetch(url):
    request = Request(url, headers={"User-Agent": AGENT, "Accept": "text/html, */*"})
    with urlopen(request, timeout=30) as response:
        payload = response.read(3_000_001)
        if len(payload) > 3_000_000:
            raise RuntimeError("Response larger than 3 MB")
        charset = response.headers.get_content_charset() or "utf-8"
        print("HTTP", response.status, "for", url, "; bytes", len(payload))
        return payload.decode(charset, "replace")

def main():
    robots = fetch(ROOT + "/robots.txt")
    policy = RobotFileParser()
    policy.parse(robots.splitlines())
    allowed = policy.can_fetch(AGENT, PAGE)
    print("robots.txt permits this user agent:", allowed)
    if not allowed:
        raise RuntimeError("Robots policy disallows this URL; stopping without access.")
    html = fetch(PAGE)
    reader = TableReader()
    reader.feed(html)
    # Inspect two public table rows to learn how comments and revision links are encoded.
    import re
    for row_html in re.findall(r"(?is)<tr[^>]*>.*?</tr>", html)[:3]:
        print("RAW HTML ROW:", row_html[:2800].replace(chr(10), " "))
    print("HTML table count:", len(reader.tables))
    for number, rows in enumerate(reader.tables):
        print("Table", number, "row count:", len(rows))
        for row in rows[:3]:
            print(" Row sample:", str(row)[:400])
    if not reader.tables or not any(len(table) > 1 for table in reader.tables):
        raise RuntimeError("No usable changes table in HTML")
    print("PROBE OK: RealmEye returned at least one usable HTML table")

if __name__ == "__main__":
    main()
