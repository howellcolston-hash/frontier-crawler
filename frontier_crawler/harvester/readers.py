"""Reading fetched bytes: HTML text and links, JSON decoding, PDF text. Each read runs in a worker process, so a parser
that crashes or hangs on hostile input costs one page, not the run.

Input contract:
    pool = ReaderPool(counts)       # counts["reader_crashes"], counts["reader_timeouts"] are kept up to date
    text, encoding, links = await pool.run(HTML_SECONDS, read_html, body, content_type, url)
    pool.close()                    # kills the workers: at the end of a run, and on error
pool.run raises ReaderFailed when the call's worker died twice or ran past the limit; other exceptions from the
reader come back as they were raised. The functions sent must be importable at module level.
This module imports only the standard library, trafilatura and lxml (pypdfium2 when a PDF is read), so workers stay small.
"""

from __future__ import annotations

import asyncio
import codecs
import faulthandler
import multiprocessing
import re
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple
from urllib.parse import urldefrag, urljoin, urlsplit, urlunsplit

import trafilatura
from trafilatura.utils import detect_encoding

READER_WORKERS = 3              # PROVISIONAL: worker processes reading pages at once
HTML_SECONDS = 60.0             # PROVISIONAL: a worker reading one HTML or JSON body for longer is killed
MARGIN_SECONDS = 30.0           # PROVISIONAL: past a reader's own time limit (PDF text) before its worker is killed
MAX_LINKS_PER_PAGE = 300        # PROVISIONAL: links kept from one page, in page order

Links = Tuple[List[Tuple[str, str]], List[str]]

# Files that are never documents. Everything else, including PDFs and spreadsheets, is followed.
_SKIP_EXTENSIONS = (
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico", ".bmp", ".css", ".js", ".mjs", ".map",
    ".woff", ".woff2", ".ttf", ".eot", ".mp3", ".mp4", ".avi", ".mov", ".webm", ".wav", ".exe", ".dmg", ".apk",
)
# Share buttons point at a form on a social site, not at a source.
_SHARE_MARKERS = ("/sharer", "/share?", "/intent/tweet", "/shareArticle", "wa.me/?text", "api.whatsapp.com/send?text")
# Legacy charsets as browsers read them: pages labelled with one routinely use its superset's extra characters.
_SUPERSETS = {"gb2312": "gb18030", "gbk": "gb18030", "big5": "big5hkscs", "shift_jis": "cp932", "euc_kr": "cp949"}


class ReaderFailed(Exception):
    """The reader's worker process died, or the read ran past its limit."""


def decode(body: bytes, content_type: str) -> Tuple[str, Optional[str]]:
    """(text, encoding read in). Valid UTF-8 as it is; else the charset the header declares, the one the page's
    <meta> declares, then a detected one. Encoding None: nothing fit, so UTF-8 with replacement characters."""
    try:
        return body.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        pass
    header = re.search(r"charset=[\"']?([\w.:-]+)", content_type, re.I)
    meta = re.search(r"<meta[^>]+charset\s*=\s*[\"']?([\w.:-]+)", body[:4096].decode("ascii", "ignore"), re.I)

    def candidates() -> Iterator[str]:
        yield from (found.group(1) for found in (header, meta) if found)
        yield from detect_encoding(body)  # only reached when no declared charset fits

    for name in candidates():
        try:
            codec = codecs.lookup(name).name
            codec = _SUPERSETS.get(codec, codec)
            return body.decode(codec), codec
        except (LookupError, UnicodeDecodeError):
            continue
    return body.decode("utf-8", errors="replace"), None


def normalise(url: str) -> str:
    """Lower-case scheme and host, drop the fragment. Empty when the URL is not http(s) or does not parse.

    A link that is dropped here is not lost: the page it was on is stored, so a better reader can pick it up later.
    """
    try:
        parts = urlsplit(urldefrag(url.strip())[0])
    except ValueError:
        return ""
    if parts.scheme.lower() not in ("http", "https") or not parts.netloc:
        return ""
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path or "/", parts.query, ""))


def extract_links(body: bytes, base_url: str, limit: int = MAX_LINKS_PER_PAGE) -> Links:
    """((url, anchor text) for each distinct link in an HTML page, in page order; links that did not parse, as written)."""
    from lxml import html as lxml_html

    try:
        tree = lxml_html.fromstring(body)
    except Exception:
        return [], []
    own = normalise(base_url)
    found: Dict[str, str] = {}
    unparseable: List[str] = []
    for anchor in tree.iter("a"):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        try:
            url = normalise(urljoin(base_url, href))
        except ValueError:
            unparseable.append(href[:500])
            continue
        if not url or url == own or url in found:
            continue
        lowered = url.lower()
        if urlsplit(lowered).path.endswith(_SKIP_EXTENSIONS) or any(marker.lower() in lowered for marker in _SHARE_MARKERS):
            continue
        found[url] = " ".join((anchor.text_content() or "").split())[:200]
        if len(found) >= limit:
            break
    return list(found.items()), unparseable


def looks_like_html(content_type: Optional[str], body: bytes) -> bool:
    content_type = (content_type or "").lower()
    if "html" in content_type:
        return True
    return not content_type and body.lstrip()[:1] == b"<"


def read_html(body: bytes, content_type: str, url: str) -> Tuple[Optional[str], Optional[str], Optional[Links]]:
    """(markdown text, encoding read in, the page's links or None when the bytes do not look like HTML)."""
    html, encoding = decode(body, content_type)
    try:
        text = trafilatura.extract(html, output_format="markdown", include_comments=False, include_tables=True)
    except Exception:  # a page trafilatura cannot read is stored without text
        text = None
    return text, encoding, extract_links(body, url) if looks_like_html(content_type, body) else None


def pdf_text(body: bytes, seconds: float) -> Tuple[str, Optional[str]]:
    """(text of every page, a note when `seconds` ran out first). Scanned pages have no text."""
    import pypdfium2

    pdf = pypdfium2.PdfDocument(body)
    try:
        parts = []
        deadline = time.monotonic() + seconds
        for index in range(len(pdf)):
            if time.monotonic() > deadline:
                return "\n".join(parts), f"text read for the first {index} of {len(pdf)} pages"
            page = pdf[index]
            textpage = page.get_textpage()
            parts.append(textpage.get_text_bounded())
            textpage.close()
            page.close()
        return "\n".join(parts), None
    finally:
        pdf.close()


def _context() -> Any:
    # forkserver, not fork: the crawler has threads and an event loop, which a forked child would inherit half-copied.
    # Workers fork from a single-threaded server that has imported only this module, so a new or replaced pool starts fast.
    context = multiprocessing.get_context("forkserver")
    context.set_forkserver_preload([__name__])
    return context


class ReaderPool:
    """Worker processes for reading. A call that finds its pool broken (its worker died, or another call's did) is
    tried once more alone, in a spare one-worker pool, so a page that crashes its reader again takes no other page with it."""

    def __init__(self, counts: Dict[str, Any], workers: int = READER_WORKERS) -> None:
        self._counts = counts
        counts.setdefault("reader_crashes", 0)
        counts.setdefault("reader_timeouts", 0)
        self._workers = {"main": workers, "spare": 1}
        self._pools: Dict[str, Optional[ProcessPoolExecutor]] = {"main": None, "spare": None}
        self._slots = asyncio.Semaphore(workers)  # each call sent finds a free worker, so its limit counts reading, not queueing
        self._retry = asyncio.Lock()

    async def run(self, limit: float, fn: Callable[..., Any], *args: Any) -> Any:
        async with self._slots:
            try:
                return await self._call("main", limit, fn, args)
            except BrokenProcessPool:
                pass
        async with self._retry:
            try:
                return await self._call("spare", limit, fn, args)
            except BrokenProcessPool:
                self._counts["reader_crashes"] += 1
                raise ReaderFailed("reader crashed") from None

    async def _call(self, name: str, limit: float, fn: Callable[..., Any], args: Tuple[Any, ...]) -> Any:
        pool = self._pools[name]
        if pool is None:
            # faulthandler: a worker that crashes prints the Python stack it crashed in (file, line, function; no arguments).
            pool = self._pools[name] = ProcessPoolExecutor(self._workers[name], mp_context=_context(), initializer=faulthandler.enable)
        try:
            return await asyncio.wait_for(asyncio.wrap_future(pool.submit(fn, *args)), limit)
        except BrokenProcessPool:
            self._drop(name, pool)
            raise
        except TimeoutError:
            self._counts["reader_timeouts"] += 1
            self._drop(name, pool)  # the only way to stop a hung reader; calls on its other workers are retried
            raise ReaderFailed("reader timed out") from None

    def _drop(self, name: str, pool: ProcessPoolExecutor) -> None:
        if self._pools[name] is pool:
            self._pools[name] = None
        # shortcut: _processes is private; Python 3.14 has ProcessPoolExecutor.kill_workers()
        for process in list((pool._processes or {}).values()):
            if process.is_alive():
                process.kill()
        pool.shutdown(wait=False, cancel_futures=True)

    def close(self) -> None:
        for name, pool in list(self._pools.items()):
            if pool is not None:
                self._drop(name, pool)
