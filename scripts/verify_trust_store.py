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
import re
from pathlib import Path

import certifi
from cryptography import x509
from cryptography.hazmat.primitives import serialization

COMPROMISED_SPKI_SHA256 = {
    "bd153ed7b0434f6886b17bce8bbe84ed340c7132d702a8f4fa318f756ecbd6f3",
    "c619e4780ed8e0abeda24ad732567de4383a7ea941338f6d1a7d1b49ac792fe9",
    "8d417db2dd8bf5e3084d1e3f196d583849d81bdd4c00c70b9d39369e96b8c782",
}
CERTIFICATE_PATTERN = re.compile(
    rb"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----", re.DOTALL
)


def verify_bundle(path: Path) -> None:
    bundle = path.read_bytes()
    certificates = CERTIFICATE_PATTERN.findall(bundle)
    begin_count = bundle.count(b"-----BEGIN CERTIFICATE-----")
    end_count = bundle.count(b"-----END CERTIFICATE-----")
    if begin_count != len(certificates) or end_count != len(certificates):
        raise SystemExit(f"CA bundle has unmatched certificate markers: {path}")
    if not certificates:
        raise SystemExit(f"CA bundle contains no certificates: {path}")

    found = set()
    for pem in certificates:
        certificate = x509.load_pem_x509_certificate(pem)
        public_key = certificate.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        fingerprint = hashlib.sha256(public_key).hexdigest()
        if fingerprint in COMPROMISED_SPKI_SHA256:
            found.add(fingerprint)

    if found:
        raise SystemExit(
            f"CA bundle {path} contains compromised SPKI fingerprints: "
            + ", ".join(sorted(found))
        )


def main() -> None:
    verify_bundle(Path("/etc/ssl/certs/ca-certificates.crt"))
    verify_bundle(Path(certifi.where()))


if __name__ == "__main__":
    main()
