"""Count the facts on a page that could join it to other records: dates, amounts, identifiers, named parties, contacts.

Regular expressions only, no model. Each kind counts distinct values, so boilerplate repeated on a page counts once.
A rough signal for steering, not a verdict: a rule or tariff text can be worth keeping with few of these.

Input contract: counts = join_keys(text) -> {"dates", "amounts", "ids", "names", "contacts": int, "per_100_words": float}
"""

from __future__ import annotations

import re
from typing import Dict, Set

MAX_CHARS = 100_000  # PROVISIONAL: only the start of a long page is scanned, so the cost per page stays small

_MONTHS = (
    "january|february|march|april|may|june|july|august|september|october|november|december"
    "|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec"
    "|janvier|février|fevrier|mars|avril|mai|juin|juillet|août|aout|septembre|octobre|novembre|décembre|decembre"
    "|enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre"
    "|janeiro|fevereiro|março|marco|maio|junho|julho|setembro|outubro|novembro|dezembro"
    "|januar|februar|märz|maerz|juni|juli|oktober|dezember"
    "|gennaio|febbraio|aprile|maggio|giugno|luglio|settembre|ottobre|dicembre"
    "|januari|februari|maret|agustus|desember"
)
_DATE = re.compile(
    r"\b\d{4}[-/.]\d{1,2}[-/.]\d{1,2}\b"                      # 2026-11-12, 2026/11/12
    r"|\b\d{1,2}[-/.]\d{1,2}[-/.](?:\d{4}|\d{2})\b"          # 12/11/2026, 12.11.26
    rf"|\b\d{{1,2}}(?:st|nd|rd|th|er|º)?\.?\s+(?:de\s+)?(?:{_MONTHS})\.?,?\s+(?:de\s+)?\d{{4}}\b"  # 12 November 2026, 12 de noviembre de 2026
    rf"|\b(?:{_MONTHS})\.?\s+\d{{1,2}}(?:st|nd|rd|th)?,?\s+\d{{4}}\b"  # November 12, 2026
    rf"|\b(?:{_MONTHS})\.?\s+\d{{4}}\b"                       # November 2026
    r"|\d{4}年\d{1,2}月(?:\d{1,2}日)?",                        # 2026年11月12日
    re.I,
)
_NUM = r"\d{1,3}(?:[,.\s]\d{3})+(?:[.,]\d+)?|\d+(?:[.,]\d+)?"
_SCALE = r"(?:\s?(?:million|billion|mln|mn|bn|m|k|millones|millions|milliards|mil))?"
_CURRENCY_CODE = r"USD|EUR|GBP|CNY|RMB|JPY|INR|CHF|CAD|AUD|BRL|MXN|ZAR|NGN|KES|GHS|AED|SAR|RUB|IDR|TRY|XOF|XAF|EGP|PKR|BDT|VND|THB|MYR|PHP|SGD|HKD|KRW|COP|CLP|PEN|ARS|UAH|PLN|SEK|NOK|DKK|CZK|HUF|RON|MAD|TZS|UGX|ETB"
_UNIT = r"t|kg|g|mt|MT|kt|Mt|tonnes?|tons?|toneladas?|bbl|barrels?|m3|m³|cbm|l|litres?|liters?|MWh|GWh|kWh|lbs?|oz|TEU|teu|dwt|DWT|ha|hectares?"
_AMOUNT = re.compile(
    rf"(?:US\$|R\$|[$€£¥₹₽₦₩₺]|\b(?:{_CURRENCY_CODE})\b)\s?(?:{_NUM}){_SCALE}"
    rf"|(?:{_NUM}){_SCALE}\s?(?:[€£¥₹₽₦]|\b(?:{_CURRENCY_CODE}|{_UNIT})\b)"
)
_ID = re.compile(
    # Letters and digits joined by separators: tender refs, licence and file numbers. Capitals only: lowercase is mostly prose or URLs.
    r"\b(?=[A-Z0-9]*[A-Z])(?=[A-Z0-9./_-]*\d)[A-Z0-9]+(?:[-/._][A-Z0-9]+)+\b"
    # One run of capitals and digits, at least 6 long with 2+ digits: VAT, company and vessel numbers.
    r"|\b(?=[A-Z0-9]*[A-Z])(?=(?:[A-Z]*\d){2})[A-Z0-9]{6,}\b"
    # A labelled number: IMO 9321483, HS 8703.23, Reg. No. 12345.
    r"|\b(?:IMO|MMSI|HS|CAS|LEI|VAT|EIN|CIK|ISIN|HRB|HRA|RCS|CNPJ|CPF|NIF|NIT|RUC|RFC|SIRET|SIREN|ABN|ACN|CIN|GSTIN|EORI|KRS|NIP|REGON|OGRN|INN|NPWP)"
    r"\s*(?:no\.?|nr\.?|number)?\s*[:#]?\s*\d[\d .\-/]{3,}\d"
    r"|\b(?:Reg(?:istration)?|Ref(?:erence)?|Licen[cs]e|Tender|Contract|Lot|File)\.?\s*(?:No|Nr|N°|Nº|#)\.?\s*:?\s*[A-Z0-9][A-Z0-9./-]*\d"
)
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_PHONE = re.compile(r"(?:\+|(?:tel|phone|fax|tél|teléfono|telefone|telefon|telp|mobile|mob)\.?\s*:?\s*)\(?\d[\d\s().-]{6,}\d", re.I)

_LEGAL = {"ltd", "llc", "sa", "gmbh", "plc", "inc", "sarl", "spa", "bv", "ag", "co", "corp", "limited", "nv", "srl", "sas",
          "oy", "ab", "kg", "pte", "pty", "lda", "tbk", "jsc", "ojsc", "llp", "lp", "bhd", "sdn", "cia", "ltda", "kft", "doo"}
_JOINERS = {"of", "de", "du", "des", "la", "le", "del", "der", "von", "van", "y", "e", "and", "&", "for", "für", "da", "do", "di"}
# A capitalised run made only of these is a menu, a banner or a sentence opening, not a party.
_COMMON = set(
    "home about us contact news privacy policy cookie cookies settings terms conditions accept all reject manage preferences"
    " sign in up log login register search menu read more services careers help faq sitemap follow subscribe newsletter"
    " english accessibility our team products solutions resources events blog media press legal notice statement use the"
    " a an this that these we you it in on at to by with from for of and back top next previous page skip main content"
    " learn share print download view details site map copyright rights reserved".split()
)
_STRIP = "()[]{}\"'“”‘’«»,;:!?|*#>_"


def _names(text: str) -> Set[str]:
    """Runs of 2+ capitalised words on one line, joined by of, de, & and the like; a word and its legal suffix is such a run."""
    found: Set[str] = set()
    for line in text.splitlines():
        run: list = []
        for token in line.split():
            word = token.strip(_STRIP).rstrip(".")
            bare = word.replace(".", "").lower()
            # Punctuation after a word ends the run, except the dot of an abbreviated legal suffix (Co., S.A.).
            ends = token[-1:] in ",;:!?|)]}\"”»" or (token.endswith(".") and bare not in _LEGAL)
            if (word[:1].isupper() and not any(ch.isdigit() for ch in word)) or (run and bare in _JOINERS):
                run.append(word)
                if ends or (bare in _LEGAL and len(run) >= 2):  # a legal suffix ends a party's name
                    run = _close(run, found)
            else:
                run = _close(run, found)
        _close(run, found)
    return found


def _close(run: list, found: Set[str]) -> list:
    while run and run[-1].lower() in _JOINERS:
        run.pop()
    while run and run[0].lower() in _COMMON:  # "The Ministry" and "Ministry" are one party
        run.pop(0)
    words = [w.rstrip(".").lower() for w in run]
    if len(run) >= 2 and not all(w in _COMMON for w in words) and len(run) <= 8:  # longer runs are headings or menus
        found.add(" ".join(words))
    return []


def _distinct(pattern: "re.Pattern[str]", text: str) -> Set[str]:
    return {" ".join(m.group(0).lower().split()) for m in pattern.finditer(text)}


def join_keys(text: str) -> Dict[str, float]:
    text = (text or "")[:MAX_CHARS]
    phones = {re.sub(r"\D", "", m.group(0)) for m in _PHONE.finditer(text)}
    counts: Dict[str, float] = {
        "dates": len(_distinct(_DATE, text)),
        "amounts": len(_distinct(_AMOUNT, text)),
        "ids": len(_distinct(_ID, text)),
        "names": len(_names(text)),
        "contacts": len(_distinct(_EMAIL, text)) + len({p for p in phones if len(p) >= 8}),
    }
    words = len(text.split())
    counts["per_100_words"] = round(100 * sum(counts.values()) / words, 2) if words else 0.0
    return counts
