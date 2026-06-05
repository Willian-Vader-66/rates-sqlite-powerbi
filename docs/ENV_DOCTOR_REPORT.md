# Environment Doctor Report

Generated: 2026-05-29T15:51:16.650537+00:00

## Python

- Version: `3.13.13`
- Executable: `C:\Projetos_Local\rates-sqlite-powerbi-git\.venv\Scripts\python.exe`
- Venv active: `True`
- Virtual env: `-`

## Certificates

- OpenSSL: `OpenSSL 3.0.19 27 Jan 2026`
- certifi installed: `True`
- certifi path: `C:\Projetos_Local\rates-sqlite-powerbi-git\.venv\Lib\site-packages\certifi\cacert.pem`
- certifi exists: `True`
- SSL_CERT_FILE: set=`True`, exists=`True`, path=`C:\Projetos_Local\rates-sqlite-powerbi-git\.venv\Lib\site-packages\certifi\cacert.pem`
- REQUESTS_CA_BUNDLE: set=`True`, exists=`True`, path=`C:\Projetos_Local\rates-sqlite-powerbi-git\.venv\Lib\site-packages\certifi\cacert.pem`
- CURL_CA_BUNDLE: set=`True`, exists=`True`, path=`C:\Projetos_Local\rates-sqlite-powerbi-git\.venv\Lib\site-packages\certifi\cacert.pem`
- truststore installed: `True`
- truststore requested: `True`
- truststore enabled: `True`
- truststore message: `truststore injected into ssl`

## Network

| Provider | URL | Status | Error type | HTTP status | Message |
|---|---|---|---|---:|---|
| FX | https://api.frankfurter.dev | OK | OK | 200 | HTTPS connection succeeded |
| Crypto | https://api.coingecko.com | OK | OK | 404 | HTTPS connection succeeded |
| Macro | https://api.bcb.gov.br | OK | OK | 404 | HTTPS connection succeeded |
| Twelve | https://api.twelvedata.com | OK | OK | 404 | HTTPS connection succeeded |

## API Keys

| Name | Present | Key length | Valid format | Suspicious | Note |
|---|---:|---:|---:|---:|---|
| TWELVE_DATA_API_KEY | True | 32 | True | False | - |
| COINGECKO_DEMO_API_KEY | False | 0 | False | False | - |
| COINGECKO_PRO_API_KEY | False | 0 | False | False | - |
| FRED_API_KEY | False | 0 | False | False | - |

## Recommendations

- Environment checks passed.
