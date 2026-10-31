#!/usr/bin/env python3
"""MCP stdio transport over SSH to private GrepMesh cloud text pilot on server 44."""
import json,sys,urllib.request
URL='http://127.0.0.1:19435/mcp'
for line in sys.stdin:
    request=None
    try:
        request=json.loads(line)
        if not isinstance(request,dict):continue
        b=json.dumps(request,ensure_ascii=False).encode()
        r=urllib.request.Request(URL,data=b,headers={'Content-Type':'application/json','Accept':'application/json, text/event-stream'})
        with urllib.request.urlopen(r,timeout=50) as response:
            payload=response.read()
        if 'id' in request and payload:print(json.dumps(json.loads(payload),ensure_ascii=False),flush=True)
    except Exception as exc:
        if isinstance(request,dict) and 'id' in request:
            print(json.dumps({'jsonrpc':'2.0','id':request['id'],'error':{'code':-32603,'message':'Cloud text pilot relay unavailable: '+type(exc).__name__}},ensure_ascii=False),flush=True)
