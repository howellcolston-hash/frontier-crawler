import asyncio
import datetime as dt
import ssl

import httpx
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import AuthorityInformationAccessOID, NameOID

from frontier_crawler.harvester import cascade


def _cert(name, issuer_name, key, issuer_key, aia=None, ca=True):
    now = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    builder = (x509.CertificateBuilder()
               .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)]))
               .issuer_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, issuer_name)]))
               .public_key(key.public_key()).serial_number(x509.random_serial_number())
               .not_valid_before(now).not_valid_after(now + dt.timedelta(days=3650))
               .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True))
    if aia:
        builder = builder.add_extension(x509.AuthorityInformationAccess(
            [x509.AccessDescription(AuthorityInformationAccessOID.CA_ISSUERS, x509.UniformResourceIdentifier(aia))]), critical=False)
    return builder.sign(issuer_key, hashes.SHA256())


def test_issuers_are_followed_and_partial_chain_trust_stays_off(monkeypatch) -> None:
    root_key, mid_key, leaf_key = (ec.generate_private_key(ec.SECP256R1()) for _ in range(3))
    mid = _cert("Mid CA", "Root CA", mid_key, root_key)
    leaf = _cert("ports.test", "Mid CA", leaf_key, mid_key, aia="http://ca.example.com/mid.der", ca=False)
    monkeypatch.setattr(cascade.ssl, "get_server_certificate", lambda addr, timeout: leaf.public_bytes(serialization.Encoding.PEM).decode())
    fetched = []
    monkeypatch.setattr(cascade.httpx, "get", lambda url, **kw: fetched.append(url) or httpx.Response(200, content=mid.public_bytes(serialization.Encoding.DER)))
    context = cascade.context_with_issuers("ports.test")
    assert fetched == ["http://ca.example.com/mid.der"]
    assert not context.verify_flags & getattr(ssl, "VERIFY_X509_PARTIAL_CHAIN", 0)
    assert any(c["subject"] == ((("commonName", "Mid CA"),),) for c in context.get_ca_certs())


def test_nothing_is_downloaded_past_a_trusted_root(monkeypatch, tmp_path) -> None:
    key = ec.generate_private_key(ec.SECP256R1())
    leaf = _cert("ports.test", "Mid CA", key, key, aia="http://ca.example.com/root.p7c", ca=False)
    monkeypatch.setattr(cascade.ssl, "get_server_certificate", lambda addr, timeout: leaf.public_bytes(serialization.Encoding.PEM).decode())
    monkeypatch.setattr(cascade.httpx, "get", lambda url, **kw: (_ for _ in ()).throw(AssertionError(url)))
    monkeypatch.setattr(cascade.certifi, "where", lambda: _bundle_with(leaf.issuer, key, tmp_path))
    assert cascade.context_with_issuers("ports.test") is None


def _bundle_with(name, key, folder):
    root = x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key()).serial_number(7) \
        .not_valid_before(dt.datetime(2026, 1, 1)).not_valid_after(dt.datetime(2036, 1, 1)).sign(key, hashes.SHA256())
    path = folder / "roots.pem"
    path.write_bytes(root.public_bytes(serialization.Encoding.PEM))
    return str(path)


class _Fake(httpx.AsyncBaseTransport):
    def __init__(self, error=""):
        self.error, self.calls = error, 0

    async def handle_async_request(self, request):
        self.calls += 1
        if self.error:
            raise httpx.ConnectError(self.error, request=request)
        return httpx.Response(200, request=request)


def _transport(default, completed, context="ctx"):
    made = {True: default, "ctx": completed}
    return cascade._ChainCompleter(complete=lambda host, port: context, make=lambda c: made[c])


def test_missing_issuer_is_completed_once_per_host_and_retried() -> None:
    default, completed = _Fake("[SSL: CERTIFICATE_VERIFY_FAILED] unable to get local issuer certificate"), _Fake()
    transport = _transport(default, completed)
    request = httpx.Request("GET", "https://ports.test/")
    for _ in range(2):
        assert asyncio.run(transport.handle_async_request(request)).status_code == 200
    assert default.calls == 1 and completed.calls == 2


def test_other_certificate_failures_are_not_retried() -> None:
    for error, context in [("[SSL: CERTIFICATE_VERIFY_FAILED] certificate has expired", "ctx"),
                           ("[SSL: CERTIFICATE_VERIFY_FAILED] unable to get local issuer certificate", None)]:
        completed = _Fake()
        transport = _transport(_Fake(error), completed, context)
        try:
            asyncio.run(transport.handle_async_request(httpx.Request("GET", "https://ports.test/")))
            raise AssertionError("expected ConnectError")
        except httpx.ConnectError:
            pass
        assert completed.calls == 0
