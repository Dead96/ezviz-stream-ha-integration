# Third-party licenses

Vendored components retain their original licenses:

- `custom_components/ezviz_stream/vendor/pyezvizapi/` — vendored from
  [RenierM26/pyEzvizApi](https://github.com/RenierM26/pyEzvizApi)
  (`main` branch, Apache License 2.0, Copyright Renier Moorcroft). See
  [`vendor/pyezvizapi/LICENSE`](custom_components/ezviz_stream/vendor/pyezvizapi/LICENSE)
  for the full license text.

  Vendored (not declared as a pip dependency) because the login and
  cloud-stream helpers this integration relies on
  (`cloud_stream.py`, `cas.py`, `client.py`'s `export_token`/`login`)
  are present on the upstream `main` branch but not yet published to the
  `pyezvizapi` package on PyPI under a matching version.
