"""
The transports. One module per way of getting a brand's records; two of them cover most brands.

``csv``        a file somebody sent -- CSV, TSV or XLSX. Entirely config-driven, so a brand that
               emails a spreadsheet is an integration with no code in it: a path and a column map.
``http_json``  a JSON endpoint, with pagination and an auth header named (never held) by the
               registry row. Also entirely config-driven.

A brand needing something else -- a crawl, a login flow, a PDF -- gets its own module here and its
own handler key. There is deliberately no generic scraper: every site's structure is its own, a
"configurable scraper" is a worse programming language for expressing it, and the reference for
how to write one against a real site is ``src/integrations/services/simpletire.py``. What a
brand-specific loader owes the rest of the package is only what ``base.LoaderContext`` asks for:
yield the records verbatim, call ``observe`` with the bytes, set ``input_label``.

Every module in this package is imported below so that registration happens by importing the
package. That import is explicit rather than a directory scan: a loader that silently fails to
register is a source that silently stops running, and an ImportError on deploy is the better of
the two failures.
"""
from src.integrations.brand_data.loaders import csv_file, http_json  # noqa: F401

__all__ = ["csv_file", "http_json"]
