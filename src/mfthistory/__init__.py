"""mfthistory: a searchable transfer history for IBM MQ Managed File Transfer.

The package reads MQ MFT transfer log messages (the XML that agents publish to the
SYSTEM.FTE topic on the coordination queue manager), folds the started, progress and
completed messages of each transfer into one record, stores them in a SQL database
and serves a scoped search API and a small self service web page.
"""

__version__ = "0.1.0"
