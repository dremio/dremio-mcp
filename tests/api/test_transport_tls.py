#
#  Copyright (C) 2017-2026 Dremio Corporation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import hashlib
import importlib.util
import ssl
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import pytest_asyncio
from aiohttp import ClientConnectorCertificateError, web
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from dremioai.api.transport import AsyncHttpClient

verify_trust_store_path = (
    Path(__file__).resolve().parents[2] / "scripts" / "verify_trust_store.py"
)
verify_trust_store_spec = importlib.util.spec_from_file_location(
    "verify_trust_store", verify_trust_store_path
)
verify_trust_store = importlib.util.module_from_spec(verify_trust_store_spec)
verify_trust_store_spec.loader.exec_module(verify_trust_store)


COMPROMISED_SPKI_SHA256 = {
    "bd153ed7b0434f6886b17bce8bbe84ed340c7132d702a8f4fa318f756ecbd6f3",
    "c619e4780ed8e0abeda24ad732567de4383a7ea941338f6d1a7d1b49ac792fe9",
    "8d417db2dd8bf5e3084d1e3f196d583849d81bdd4c00c70b9d39369e96b8c782",
}


def create_certificate(tmp_path: Path) -> tuple[Path, Path, str]:
    private_key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(timezone.utc) - timedelta(minutes=1))
        .not_valid_after(datetime.now(timezone.utc) + timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False
        )
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(private_key, hashes.SHA256())
    )
    cert_path = tmp_path / "localhost.crt"
    key_path = tmp_path / "localhost.key"
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    spki = certificate.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return cert_path, key_path, hashlib.sha256(spki).hexdigest()


@pytest_asyncio.fixture
async def https_server(tmp_path):
    cert_path, key_path, _ = create_certificate(tmp_path)
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain(cert_path, key_path)

    async def handle(_request):
        return web.json_response({"ok": True})

    app = web.Application()
    app.router.add_route("*", "/value", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=ssl_context)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    try:
        yield f"https://localhost:{port}/", cert_path
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["get", "post"])
async def test_async_http_client_rejects_untrusted_https_certificate(
    https_server, method, monkeypatch
):
    uri, _ = https_server
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    monkeypatch.delenv("SSL_CERT_DIR", raising=False)
    client = AsyncHttpClient(uri, "test-token")

    with pytest.raises(ClientConnectorCertificateError):
        await getattr(client, method)(
            "value", ssl=True, **({"body": {}} if method == "post" else {})
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["get", "post"])
async def test_async_http_client_accepts_certificate_in_validated_trust_context(
    https_server, method
):
    uri, cert_path = https_server
    trusted_context = ssl.create_default_context(cafile=str(cert_path))
    client = AsyncHttpClient(uri, "test-token")

    result = await getattr(client, method)(
        "value",
        ssl=trusted_context,
        **({"body": {}} if method == "post" else {}),
    )

    assert result == {"ok": True}


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["get", "post"])
async def test_async_http_client_ssl_defaults_to_false(https_server, method):
    uri, _ = https_server
    client = AsyncHttpClient(uri, "test-token")

    result = await getattr(client, method)(
        "value", **({"body": {}} if method == "post" else {})
    )

    assert result == {"ok": True}


def test_trust_store_guard_uses_all_compromised_spkis():
    assert verify_trust_store.COMPROMISED_SPKI_SHA256 == COMPROMISED_SPKI_SHA256


def test_trust_store_guard_rejects_a_denied_spki(tmp_path, monkeypatch):
    cert_path, _, spki = create_certificate(tmp_path)
    monkeypatch.setattr(verify_trust_store, "COMPROMISED_SPKI_SHA256", {spki})
    with pytest.raises(SystemExit, match="compromised SPKI"):
        verify_trust_store.verify_bundle(cert_path)


def test_trust_store_guard_rejects_empty_malformed_and_clean_bundles(
    tmp_path, monkeypatch
):
    path = tmp_path / "bundle.pem"
    monkeypatch.setattr(verify_trust_store, "COMPROMISED_SPKI_SHA256", set())

    path.write_bytes(b"")
    with pytest.raises(SystemExit, match="no certificates"):
        verify_trust_store.verify_bundle(path)

    path.write_bytes(b"-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----")
    with pytest.raises(ValueError):
        verify_trust_store.verify_bundle(path)

    path.write_bytes(b"-----BEGIN CERTIFICATE-----\nmissing end")
    with pytest.raises(SystemExit, match="unmatched certificate markers"):
        verify_trust_store.verify_bundle(path)

    cert_path, _, _ = create_certificate(tmp_path)
    verify_trust_store.verify_bundle(cert_path)
