# Security policy

## Reporting a vulnerability

Please do not open a public issue. Use GitHub's private reporting: open the **Security** tab of this repository and choose **Report a vulnerability**. Include the version (`sic --version`), how the cache is deployed, and the steps to reproduce.

The report is acknowledged, and you are kept informed while a fix is prepared. A fix is released as a patch version, with a note in `CHANGELOG.md`.

## Supported versions

The latest release. Older releases get fixes only for vulnerabilities that cannot be avoided by upgrading.

## What the cache trusts

The README's sections "Upstream fallback" and "TLS" describe the security boundary. In short: SIC does not check who is asking. With mutual TLS, every holder of a certificate signed by the configured CA can read anything in the cache, including content that was first fetched with someone's registry credentials. Treat the client CA as the access boundary.
