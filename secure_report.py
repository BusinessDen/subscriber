#!/usr/bin/env python3
"""
BD Revenue -- report encryption helper.

Lets the structure report be produced from a PUBLIC repo without leaking it.
Actions artifacts on a public repository are downloadable by anyone, so the
report is encrypted on the runner and only decrypted on your machine.

Uses PBKDF2-HMAC-SHA256 (300k iterations) to derive a key from a passphrase,
then Fernet (AES-128-CBC + HMAC-SHA256) for authenticated encryption. No
hand-rolled crypto.

Requires: pip install cryptography

Encrypt (runner does this automatically):
    REPORT_PASSPHRASE=... python secure_report.py encrypt in.txt out.enc

Decrypt (you, locally -- prompts for the passphrase):
    python secure_report.py decrypt out.enc report.txt
"""

import base64
import getpass
import os
import sys

try:
    from cryptography.fernet import Fernet, InvalidToken
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
except ImportError:
    sys.exit("Missing dependency. Run: pip install cryptography")

MAGIC = b"BDENC1"
ITERATIONS = 300000
SALT_BYTES = 16


def derive(passphrase, salt):
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=ITERATIONS,
    )
    return base64.urlsafe_b64encode(kdf.derive(passphrase.encode("utf-8")))


def encrypt(src, dst):
    passphrase = os.environ.get("REPORT_PASSPHRASE")
    if not passphrase:
        sys.exit("REPORT_PASSPHRASE not set.")
    if len(passphrase) < 12:
        sys.exit("Passphrase too short. Use at least 12 characters.")

    with open(src, "rb") as fh:
        plaintext = fh.read()

    salt = os.urandom(SALT_BYTES)
    token = Fernet(derive(passphrase, salt)).encrypt(plaintext)

    with open(dst, "wb") as fh:
        fh.write(MAGIC + b"\n")
        fh.write(base64.b64encode(salt) + b"\n")
        fh.write(token)

    sys.stderr.write(
        "Encrypted {} bytes -> {}\n".format(len(plaintext), dst))


def decrypt(src, dst):
    with open(src, "rb") as fh:
        lines = fh.read().split(b"\n", 2)

    if len(lines) < 3 or lines[0] != MAGIC:
        sys.exit("Not a BDENC1 file.")

    salt = base64.b64decode(lines[1])
    token = lines[2]

    passphrase = getpass.getpass("Passphrase: ")

    try:
        plaintext = Fernet(derive(passphrase, salt)).decrypt(token)
    except InvalidToken:
        sys.exit("Wrong passphrase, or the file was altered in transit.")

    with open(dst, "wb") as fh:
        fh.write(plaintext)

    print("Decrypted -> {} ({} bytes)".format(dst, len(plaintext)))


def main():
    if len(sys.argv) != 4 or sys.argv[1] not in ("encrypt", "decrypt"):
        sys.exit(__doc__)
    mode, src, dst = sys.argv[1], sys.argv[2], sys.argv[3]
    if mode == "encrypt":
        encrypt(src, dst)
    else:
        decrypt(src, dst)


if __name__ == "__main__":
    main()
