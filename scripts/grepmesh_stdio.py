#!/usr/bin/env python3
"""Serial NDJSON MCP bridge to GrepMesh HTTP; stdout contains JSON-RPC only.

No retries, proxy environment, redirects, token files or background processes.
Use --bearer-env NAME to read an existing token without printing its value.
"""
import argparse
import http.client
import ipaddress
import json
import os
import sys
import time
import urllib.parse

DEFAULT_ENDPOINT = "http://127.0.0.1:9419/mcp"
MAX_REQUEST = 1024 * 1024
MAX_RESPONSE = 2 * 1024 * 1024
PROTOCOL = "2026-07-28"


class BridgeError(Exception):
    """Deliberately secret-free public error."""


def reject_nonfinite(_value):
    raise ValueError("non-finite JSON number")


def endpoint_parts(endpoint, allow_remote=False):
    try:
        parts = urllib.parse.urlsplit(endpoint)
        port = parts.port
        if parts.username or parts.password or parts.query or parts.fragment:
            raise ValueError()
        host = parts.hostname
        if not host or parts.scheme not in ("http", "https"):
            raise ValueError()
        if host.lower() == "localhost":
            host = "127.0.0.1"  # no DNS/proxy ambiguity for the default local boundary
        try:
            local = ipaddress.ip_address(host).is_loopback
        except ValueError:
            local = False
        if not local and (not allow_remote or parts.scheme != "https"):
            raise ValueError()
        if not 1 <= (port or (443 if parts.scheme == "https" else 80)) <= 65535:
            raise ValueError()
        path = parts.path or "/mcp"
        if any(ord(character) < 32 for character in endpoint):
            raise ValueError()
        return parts.scheme, host, port, path
    except (ValueError, TypeError):
        raise BridgeError("endpoint must be loopback HTTP(S), without credentials/query/fragment; remote requires --allow-remote and HTTPS") from None


def rpc_error(identifier, code, message):
    return {"jsonrpc": "2.0", "id": identifier, "error": {"code": code, "message": message}}


class Bridge:
    def __init__(self, endpoint=DEFAULT_ENDPOINT, bearer=None, timeout=30.0,
                 max_request=MAX_REQUEST, max_response=MAX_RESPONSE, allow_remote=False):
        self.endpoint = endpoint_parts(endpoint, allow_remote)
        self.timeout = timeout
        self.max_request = max_request
        self.max_response = max_response
        self.protocol = PROTOCOL
        if bearer and ("\r" in bearer or "\n" in bearer):
            raise BridgeError("invalid bearer environment value")
        self.bearer = bearer

    def forward(self, message):
        method = message["method"]
        headers = {"Content-Type": "application/json", "Accept": "application/json",
                   "MCP-Protocol-Version": self.protocol, "Mcp-Method": method}
        if method == "tools/call":
            name = message.get("params", {}).get("name")
            if not isinstance(name, str) or not name or len(name) > 256:
                raise BridgeError("tools/call requires a tool name")
            headers["Mcp-Name"] = name
        if self.bearer:
            headers["Authorization"] = "Bearer " + self.bearer
        body = json.dumps(message, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
        if len(body) > self.max_request:
            raise BridgeError("request exceeds configured byte limit")
        scheme, host, port, path = self.endpoint
        constructor = http.client.HTTPSConnection if scheme == "https" else http.client.HTTPConnection
        connection = constructor(host, port, timeout=self.timeout)
        deadline = time.monotonic() + self.timeout
        try:
            connection.request("POST", path, body=body, headers=headers)
            response = connection.getresponse()
            if response.status not in (200, 202, 204):
                raise BridgeError("upstream HTTP status {} (not retried)".format(response.status))
            advertised = response.getheader("Content-Length")
            if advertised and int(advertised) > self.max_response:
                raise BridgeError("response exceeds configured byte limit")
            chunks = []
            size = 0
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BridgeError("upstream request timed out (not retried)")
                if connection.sock is not None:
                    connection.sock.settimeout(remaining)
                chunk = response.read1(min(65536, self.max_response + 1 - size))
                if not chunk:
                    break
                size += len(chunk)
                if size > self.max_response:
                    raise BridgeError("response exceeds configured byte limit")
                chunks.append(chunk)
            # Notifications never cause responses on stdio, including upstream
            # implementations that incorrectly return a JSON envelope for them.
            if "id" not in message:
                return None
            if not chunks:
                raise BridgeError("upstream returned no response for a request")
            envelope = json.loads(b"".join(chunks), parse_constant=reject_nonfinite)
            if (not isinstance(envelope, dict) or envelope.get("jsonrpc") != "2.0"
                    or "id" not in envelope or type(envelope["id"]) is not type(message["id"])
                    or envelope["id"] != message["id"]
                    or ("result" in envelope) == ("error" in envelope)):
                raise BridgeError("invalid upstream JSON-RPC envelope")
            if method == "initialize" and isinstance(envelope.get("result"), dict):
                negotiated = envelope["result"].get("protocolVersion")
                if negotiated in ("2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25", "2026-07-28"):
                    self.protocol = negotiated
            return envelope
        except BridgeError:
            raise
        except (OSError, ValueError, http.client.HTTPException):
            # Exception strings can include headers, endpoint credentials or
            # upstream bodies. Never log/return them to a model or the terminal.
            raise BridgeError("upstream network/protocol failure (not retried)") from None
        finally:
            connection.close()

    def handle(self, raw):
        try:
            message = json.loads(raw, parse_constant=reject_nonfinite)
        except (ValueError, UnicodeError):
            return rpc_error(None, -32700, "invalid JSON")
        if (not isinstance(message, dict) or message.get("jsonrpc") != "2.0"
                or not isinstance(message.get("method"), str)
                or not message["method"] or len(message["method"]) > 256
                or ("params" in message and not isinstance(message["params"], dict))
                or ("id" in message and (isinstance(message["id"], bool)
                    or not isinstance(message["id"], (str, int, type(None)))))):
            if isinstance(message, dict) and "id" not in message and isinstance(message.get("method"), str):
                print("GrepMesh invalid notification ignored", file=sys.stderr)
                return None
            return rpc_error(None, -32600, "invalid JSON-RPC request")
        try:
            return self.forward(message)
        except BridgeError as error:
            if "id" not in message:
                print("GrepMesh notification forwarding failed; not retried", file=sys.stderr)
                return None
            return rpc_error(message["id"], -32001, str(error))


def serve(bridge, source, destination):
    while True:
        raw = source.readline(bridge.max_request + 2)
        if not raw:
            return
        if len(raw) > bridge.max_request:
            # Drain the rest without retaining it. Do not invent a response ID
            # for an oversized possible notification; terminate this transport.
            print("GrepMesh stdio request byte limit exceeded", file=sys.stderr)
            return
        if not raw.strip():
            continue
        response = bridge.handle(raw)
        if response is not None:
            destination.write(json.dumps(response, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode() + b"\n")
            destination.flush()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--bearer-env", help="environment variable containing an existing bearer")
    parser.add_argument("--allow-remote", action="store_true", help="explicitly permit a non-loopback HTTPS endpoint")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--max-request-bytes", type=int, default=MAX_REQUEST)
    parser.add_argument("--max-response-bytes", type=int, default=MAX_RESPONSE)
    args = parser.parse_args()
    try:
        if not 0 < args.timeout <= 300 or not 1024 <= args.max_request_bytes <= 8 * MAX_REQUEST or not 1024 <= args.max_response_bytes <= 16 * MAX_RESPONSE:
            raise BridgeError("invalid timeout or byte limits")
        bearer = None
        if args.bearer_env:
            bearer = os.environ.get(args.bearer_env)
            if not bearer:
                raise BridgeError("requested bearer environment variable is missing or empty")
        bridge = Bridge(args.endpoint, bearer, args.timeout, args.max_request_bytes,
                        args.max_response_bytes, args.allow_remote)
        serve(bridge, sys.stdin.buffer, sys.stdout.buffer)
    except (BridgeError, BrokenPipeError) as error:
        if isinstance(error, BridgeError):
            print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
