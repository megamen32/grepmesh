# Private signed bridge candidate

This stdio child is source-only: it is not registered or enabled by this
change and does not constitute a production cutover. The bridge trusts
filesystem ACL metadata supplied by its authenticated ShellMCP parent; a
caller with direct access to its stdio could forge that metadata. It must only
run behind the verified parent and must not be exposed as a public endpoint.

Historical server-44 reads require delegated ACL and signed transport;
mixed-host search is rejected and historical results are marked partial.
Before registration or cutover, prove allowed and denied reads through a real
authorized GPTAdmin profile and establish historical and live-data parity.
