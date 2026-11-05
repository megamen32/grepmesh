#!/usr/bin/env python3
"""Oversized historical body support for the private cloud FTS review.

Bodies larger than the 1 MiB fulltext-shard limit are excluded from search
shards by the publisher. This module turns them into two immutable cloud
objects instead: newline-aligned content-addressed chunk files, plus one
small per-snapshot oversize index manifest reached through a pointer file.
The multi-cloud reader serves read_text for oversized paths lazily from the
index, chunk by chunk, with the same response shape and the same guards as
the 1 MiB shard reader. The native SQLite indexes, existing scope manifests
and the default mesh are untouched; nothing here grows any SSD.

Line semantics match the shard reader exactly: logical lines are
str.splitlines lines. Chunks are cut only between lines; a single line
longer than the chunk target is cut mid-piece into byte-bounded sub-pieces
that share one line number on read, so no line is fabricated, split or
lost. ``ends_mid_line`` means "the next chunk continues this logical line".
"""
from __future__ import annotations
import base64, bisect, hashlib, json, os, re, threading, time, zlib
from pathlib import Path as P
from cloud_index import MAX_DOCUMENT, MIB, valid_key, json_bytes, digest, \
    regex_match_positions, regex_can_span_newlines

CHUNK_TARGET_BYTES = 512 * 1024
MAX_CHUNK_BYTES = 8 * MIB
MAX_OVERSIZE_DOCUMENT = 64 * MIB
MAX_OVERSIZE_MANIFEST = 48 * MIB
MAX_POINTER_BYTES = 65536
OVERSIZE_FORMAT = 1
_TERMINATORS = '\n\r\v\f\x1c\x1d\x1e\x85  '
_HEX64 = re.compile(r'[0-9a-f]{64}')
_IDENT = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}')


def _ident(value,label):
    if not isinstance(value,str) or not _IDENT.fullmatch(value):
        raise ValueError('Invalid oversize '+label)
    return value


def _terminator_length(piece):
    """Bytes of the Unicode terminator at the end of piece (0 if none)."""
    if piece.endswith('\r\n'):return 2
    return 1 if piece and piece[-1] in _TERMINATORS else 0


def strip_terminator(piece):
    """Strip one splitlines terminator, exactly as the shard reader does."""
    length=_terminator_length(piece)
    return piece[:len(piece)-length] if length else piece


def byte_slices(piece,limit):
    """Split str into non-empty pieces of at most limit UTF-8 bytes each."""
    if limit < 1:return []
    out=[];start=0;acc=0
    for index,char in enumerate(piece):
        length=len(char.encode('utf-8'))
        if index>start and acc+length>limit:
            out.append(piece[start:index]);start=index;acc=length
        else:acc+=length
    if start<len(piece):out.append(piece[start:])
    return out


def plan_chunks(body,chunk_bytes=CHUNK_TARGET_BYTES):
    """Split a decoded body into chunks that never re-cut a line boundary.

    Returns chunk descriptors with text, line/byte bookkeeping and a sha256;
    ``start_line`` is the line number that CONTAINS the chunk's first byte.
    Consecutive chunk ``start_byte`` values are contiguous, and chunk k+1
    starts mid-line exactly when chunk k ends mid-line.
    """
    if not isinstance(body,str) or not 1<=chunk_bytes<=MAX_CHUNK_BYTES:
        raise ValueError('Invalid oversize chunking request')
    # Expand every logical line into byte-bounded fragments; only a line
    # longer than the target becomes more than one fragment.
    fragments=[]
    for piece in body.splitlines(keepends=True):
        length=len(piece.encode('utf-8'))
        if length<=chunk_bytes:
            fragments.append((piece,length,False,False,True));continue
        subs=byte_slices(piece,chunk_bytes)
        count=len(subs)
        for j,sub in enumerate(subs):
            fragments.append((sub,len(sub.encode('utf-8')),j>0,j<count-1,j==count-1))
    chunks=[];current=[];current_bytes=0
    current_line=None;current_byte=0;current_mid=False;current_ends=False
    offset=0;line=1
    def emit(text,start_line,start_byte,starts_mid,ends_mid):
        encoded=text.encode('utf-8')
        chunks.append({'index':len(chunks)+1,'start_line':start_line,
                       'start_byte':start_byte,'starts_mid_line':starts_mid,
                       'ends_mid_line':ends_mid,'text':text,
                       'raw_bytes':len(encoded),
                       'sha256':hashlib.sha256(encoded).hexdigest()})
    for text,length,starts_mid,ends_mid,advance_line in fragments:
        if current and current_bytes+length>chunk_bytes:
            emit(''.join(current),current_line,current_byte,current_mid,current_ends)
            current=[];current_bytes=0
        if not current:
            current_line=line;current_byte=offset
            current_mid=starts_mid;current_ends=ends_mid
        current.append(text);current_bytes+=length;current_ends=ends_mid
        offset+=length
        if advance_line:line+=1
    if current:
        emit(''.join(current),current_line,current_byte,current_mid,current_ends)
    return chunks


def oversize_pointer_key(host,snapshot):
    return valid_key('oversize/'+_ident(host,'host')+'/' +
                     _ident(snapshot,'snapshot')+'/latest.json')


def publish_oversize(store,host,snapshot,docs,chunk_bytes=CHUNK_TARGET_BYTES):
    """Publish immutable chunk objects plus one oversize index manifest.

    docs is an iterable of {'path','sha256','body'} for oversized rows.
    Chunk keys are content-addressed, so a re-run publishes identical bytes
    in place; only manifest/pointer files carry fresh accounting. Chunk
    objects land before the manifest and pointer, so a published pointer
    never references missing chunks.
    """
    host=_ident(host,'host');snapshot=_ident(snapshot,'snapshot')
    if not 1<=chunk_bytes<=MAX_CHUNK_BYTES:
        raise ValueError('Invalid oversize chunk bound')
    records=[];raw_total=0;packed_total=0;chunk_total=0
    for doc in docs:
        path=doc.get('path');body=doc.get('body')
        if (not isinstance(path,str) or not path.startswith('/') or
                chr(0) in path or len(path.encode('utf-8'))>4096 or
                '..' in P(path).parts or os.path.normpath(path)!=path):
            raise ValueError('Oversize record path must be a normalized absolute path')
        if not isinstance(body,str):
            raise ValueError('Oversize body must already be decoded text')
        raw=body.encode('utf-8')
        if len(raw)<=MAX_DOCUMENT:
            raise ValueError('Oversize publisher received a body under the shard limit')
        if len(raw)>MAX_OVERSIZE_DOCUMENT:
            raise ValueError('Oversize body exceeds the published chunk cap')
        body_sha=digest(raw)
        declared=doc.get('sha256')
        if isinstance(declared,str) and declared and declared!=body_sha:
            raise ValueError('Oversize body digest mismatch')
        chunks=plan_chunks(body,chunk_bytes)
        record={'path':path,'body_sha256':body_sha,'size_bytes':len(raw),
                'line_count':len(body.splitlines()),
                'chunk_target_bytes':chunk_bytes,'chunks':[]}
        for chunk in chunks:
            packed=zlib.compress(chunk['text'].encode('utf-8'),3)
            descriptor={'index':chunk['index'],
                        'start_line':chunk['start_line'],
                        'start_byte':chunk['start_byte'],
                        'starts_mid_line':chunk['starts_mid_line'],
                        'ends_mid_line':chunk['ends_mid_line'],
                        'raw_bytes':chunk['raw_bytes'],
                        'sha256':chunk['sha256'],
                        'packed_bytes':len(packed),
                        'packed_sha256':digest(packed),
                        'key':valid_key('oversize/'+host+'/'+snapshot+'/'+
                                        body_sha+'/chunk-'+
                                        format(chunk['index'],'04d')+'.zlib')}
            record['chunks'].append(descriptor)
            store.put(descriptor['key'],packed)
        records.append(record)
        raw_total+=len(raw)
        packed_total+=sum(x['packed_bytes'] for x in record['chunks'])
        chunk_total+=len(record['chunks'])
    manifest={'kind':'oversize_index','format':OVERSIZE_FORMAT,'host':host,
              'snapshot':snapshot,'documents':len(records),'records':records,
              'chunk_target_bytes':chunk_bytes,'chunks':chunk_total,
              'raw_bytes':raw_total,'packed_bytes':packed_total,
              'generated_at':time.time()}
    blob=json_bytes(manifest)
    if len(blob)>MAX_OVERSIZE_MANIFEST:
        raise ValueError('Oversize index manifest too large')
    manifest_key=valid_key('oversize/'+host+'/'+snapshot+'/manifest.json')
    store.put(manifest_key,blob)
    pointer={'kind':'oversize_pointer','manifest':manifest_key,
             'sha256':digest(blob),'documents':len(records),
             'host':host,'snapshot':snapshot}
    pointer_key=oversize_pointer_key(host,snapshot)
    store.put(pointer_key,json_bytes(pointer))
    return {'pointer_key':pointer_key,'manifest_key':manifest_key,
            'manifest_sha256':digest(blob),'documents':len(records),
            'chunks':chunk_total,'raw_bytes':raw_total,
            'packed_bytes':packed_total}


def _validate_path(path):
    if (not isinstance(path,str) or not path.startswith('/') or
            chr(0) in path or len(path.encode('utf-8'))>4096 or
            '..' in P(path).parts or os.path.normpath(path)!=path):
        raise ValueError('A normalized absolute historical path is required')
    return path


def _validate_line_args(start_line,end_line,deadline_seconds):
    if (not isinstance(start_line,int) or isinstance(start_line,bool) or
            start_line<1 or start_line>100000000):
        raise ValueError('Invalid start_line')
    if end_line is not None and (not isinstance(end_line,int) or
            isinstance(end_line,bool) or end_line<1 or end_line>100000000):
        raise ValueError('Invalid end_line')
    if deadline_seconds<=0 or deadline_seconds>120:
        raise ValueError('Invalid read deadline')


def _validate_roots(path,authorized_roots):
    def under(p,root):return p==root or p.startswith(root.rstrip('/')+'/')
    if authorized_roots is None:return
    if (not isinstance(authorized_roots,list) or
            not all(isinstance(x,str) and x.startswith('/') and
                    '..' not in P(x).parts for x in authorized_roots)):
        raise PermissionError('Invalid verified filesystem read roots')
    if not any(under(path,root) for root in authorized_roots):
        raise PermissionError('Filesystem ACL denies historical path')


def _chunk_key(host,snapshot,body_sha,index):
    return valid_key('oversize/'+host+'/'+snapshot+'/'+body_sha+
                     '/chunk-'+format(index,'04d')+'.zlib')


class OversizeIndex:
    """Lazy lazy loader for one historical oversize index (host+snapshot).

    The pointer and manifest are loaded on first use and re-validated every
    refresh; chunk data is never held across calls.
    """
    def __init__(self,store,host,snapshot):
        self.store=store;self.host=_ident(host,'host')
        self.snapshot=_ident(snapshot,'snapshot')
        self.manifest=None;self.by_path=None
        self.lock=threading.RLock()
    @property
    def pointer_key(self):return oversize_pointer_key(self.host,self.snapshot)
    def clear(self):
        with self.lock:self.manifest=None;self.by_path=None
    def refresh(self):
        """Load pointer+manifest and validate the full chain of bookkeeping."""
        pointer=json.loads(self.store.get(self.pointer_key,MAX_POINTER_BYTES))
        if not isinstance(pointer,dict) or pointer.get('kind')!='oversize_pointer':
            raise ValueError('Invalid oversize pointer')
        if pointer.get('host')!=self.host or pointer.get('snapshot')!=self.snapshot:
            raise ValueError('Oversize pointer identity mismatch')
        manifest_key=pointer.get('manifest')
        if (not isinstance(manifest_key,str) or
                manifest_key!=valid_key('oversize/'+self.host+'/'+self.snapshot+
                                        '/manifest.json')):
            raise ValueError('Invalid oversize manifest key')
        blob=self.store.get(manifest_key,MAX_OVERSIZE_MANIFEST)
        if digest(blob)!=pointer.get('sha256'):
            raise ValueError('Oversize manifest checksum mismatch')
        m=json.loads(blob)
        if not isinstance(m,dict) or m.get('kind')!='oversize_index' or \
                m.get('format')!=OVERSIZE_FORMAT:
            raise ValueError('Invalid oversize manifest')
        if m.get('host')!=self.host or m.get('snapshot')!=self.snapshot:
            raise ValueError('Oversize manifest identity mismatch')
        records=m.get('records')
        if not isinstance(records,list) or m.get('documents')!=len(records):
            raise ValueError('Oversize manifest row accounting failed')
        by_path={}
        for record in records:
            self._validate_record(record)
            if record['path'] in by_path:
                raise ValueError('Duplicate oversize record for one path')
            by_path[record['path']]=record
        with self.lock:
            self.manifest=m;self.by_path=by_path
        return m
    def _validate_record(self,record):
        if not isinstance(record,dict):raise ValueError('Invalid oversize record')
        path=_validate_path(record.get('path'))
        body_sha=record.get('body_sha256')
        if not isinstance(body_sha,str) or not _HEX64.fullmatch(body_sha):
            raise ValueError('Invalid oversize body digest')
        size=record.get('size_bytes')
        if not isinstance(size,int) or isinstance(size,bool) or \
                size<=MAX_DOCUMENT or size>MAX_OVERSIZE_DOCUMENT:
            raise ValueError('Invalid oversize size')
        line_count=record.get('line_count')
        if not isinstance(line_count,int) or isinstance(line_count,bool) or line_count<1:
            raise ValueError('Invalid oversize line count')
        chunks=record.get('chunks')
        if not isinstance(chunks,list) or not chunks:
            raise ValueError('Oversize record has no chunks')
        last_line=None;bytes_total=0;previous=None
        for k,chunk in enumerate(chunks):
            if not isinstance(chunk,dict):
                raise ValueError('Invalid oversize chunk descriptor')
            if chunk.get('index')!=k+1:
                raise ValueError('Oversize chunk sequence mismatch')
            for field in ('sha256','packed_sha256'):
                value=chunk.get(field)
                if not isinstance(value,str) or not _HEX64.fullmatch(value):
                    raise ValueError('Invalid oversize chunk '+field)
            if chunk.get('key')!=_chunk_key(self.host,self.snapshot,body_sha,k+1):
                raise ValueError('Oversize chunk key identity mismatch')
            raw=chunk.get('raw_bytes');packed=chunk.get('packed_bytes')
            for value,limit in ((raw,MAX_CHUNK_BYTES),(packed,MAX_CHUNK_BYTES)):
                if not isinstance(value,int) or isinstance(value,bool) or \
                        value<1 or value>limit:
                    raise ValueError('Invalid oversize chunk byte bound')
            line=chunk.get('start_line')
            if not isinstance(line,int) or isinstance(line,bool) or line<1:
                raise ValueError('Invalid oversize chunk line')
            if last_line is not None and line<last_line:
                raise ValueError('Oversize chunk line regression')
            last_line=line
            byte=chunk.get('start_byte')
            if not isinstance(byte,int) or isinstance(byte,bool) or byte<0:
                raise ValueError('Invalid oversize chunk byte offset')
            mid=chunk.get('starts_mid_line');end_mid=chunk.get('ends_mid_line')
            if not isinstance(mid,bool) or not isinstance(end_mid,bool):
                raise ValueError('Invalid oversize chunk flags')
            if previous is None:
                if k==0:
                    if mid or byte!=0 or line!=1:
                        raise ValueError('Oversize chunk one must open the body')
                else:raise AssertionError('unreachable')
            else:
                if mid!=previous['ends_mid_line']:
                    raise ValueError('Oversize chunk continuation chain mismatch')
                if byte!=previous['start_byte']+previous['raw_bytes']:
                    raise ValueError('Oversize chunk byte continuity broken')
            bytes_total+=raw;previous=chunk
        if bytes_total!=size:
            raise ValueError('Oversize record byte accounting mismatch')
        if chunks[-1]['ends_mid_line'] or last_line>line_count:
            raise ValueError('Oversize record must end on a line boundary')
    def _require(self):
        if self.manifest is None:self.refresh()
        if self.by_path is None:self.refresh()
        return self.manifest,self.by_path
    def read_text(self,path,start_line=1,end_line=None,authorized_roots=None,
                  deadline_seconds=25,byte_offset=None,byte_limit=None,
                  expected_body_sha256=None):
        """Read lines of one oversized historical body from immutable chunks.

        Same auth, deadline and response shape as the 1 MiB shard reader.
        Missing/corrupt/timeout cases are errors, never authoritative empty
        text; a chunk chain is verified byte-for-byte before its lines are
        reconstructed.
        """
        path=_validate_path(path)
        _validate_line_args(start_line,end_line,deadline_seconds)
        _validate_roots(path,authorized_roots)
        started=time.monotonic()
        with self.lock:
            manifest,by_path=self._require()
            record=by_path.get(path)
            if record is None:
                raise FileNotFoundError('Historical path is not in the oversize index')
            chunks=record['chunks'];line_count=record['line_count']
            if byte_offset is not None or byte_limit is not None or expected_body_sha256 is not None:
                if start_line != 1 or end_line is not None:
                    raise ValueError('Byte and line ranges cannot be combined')
                if type(byte_offset) is not int or not 0 <= byte_offset <= record['size_bytes']:
                    raise ValueError('Invalid historical byte offset')
                if byte_limit is None:byte_limit=256*1024
                if type(byte_limit) is not int or not 1 <= byte_limit <= 256*1024:
                    raise ValueError('Invalid historical byte limit')
                if byte_offset and expected_body_sha256 is None:
                    raise ValueError('Continuation requires expected body SHA-256')
                if expected_body_sha256 is not None and expected_body_sha256 != record['body_sha256']:
                    raise ValueError('Historical body SHA-256 changed')
                if byte_offset == record['size_bytes']:
                    raw=b''
                else:
                    k=bisect.bisect_right([c['start_byte'] for c in chunks],byte_offset)-1
                    if time.monotonic()-started>=deadline_seconds:
                        raise TimeoutError('Historical oversize read deadline')
                    chunk=chunks[k]
                    data=self._load_chunk(chunk).encode('utf-8')
                    local=byte_offset-chunk['start_byte']
                    raw=data[local:local+byte_limit]
                next_offset=byte_offset+len(raw)
                return {'path':path,'source_host':self.host,
                        'body_sha256':record['body_sha256'],
                        'size_bytes':record['size_bytes'],'byte_offset':byte_offset,
                        'next_byte_offset':next_offset,'eof':next_offset==record['size_bytes'],
                        'data_base64':base64.b64encode(raw).decode('ascii'),
                        'part_sha256':digest(raw),'snapshot_at':manifest.get('generated_at')}
            if start_line>line_count:
                return self._empty_result(path,start_line)
            stop=max(start_line,end_line if end_line is not None else line_count)
            if stop<start_line:stop=start_line
            keys=[chunk['start_line'] for chunk in chunks]
            k0=bisect.bisect_right(keys,start_line)-1
            # The region must begin on a line boundary: walk back over a
            # mid-line chunk chain to the chunk that opens the line.
            while k0>0 and chunks[k0]['starts_mid_line']:k0-=1
            k1=k0
            while k1+1<len(chunks) and chunks[k1+1]['start_line']<=stop:k1+=1
            region=[]
            for k in range(k0,k1+1):
                if time.monotonic()-started>=deadline_seconds:
                    raise TimeoutError('Historical oversize read deadline')
                chunk=chunks[k]
                text=self._load_chunk(chunk)
                region.append((chunk,text))
            # The region is byte-contiguous and starts exactly on a line
            # boundary, so splitlines over it preserves shard-reader parity.
            texts=region[0][1] if len(region)==1 else ''.join(x[1] for x in region)
            base=region[0][0]['start_line']
            selected=[]
            for i,lines_text in enumerate(texts.splitlines()):
                number=base+i
                if start_line<=number<=stop:
                    selected.append({'line_number':number,'text':lines_text})
            end_line=min(stop,line_count) if line_count>=start_line else start_line
            if sum(len(x['text'].encode('utf-8')) for x in selected)>MAX_DOCUMENT:
                raise RuntimeError('Historical text range exceeds 1 MiB')
            return {'path':path,'source_host':self.host,
                    'body_sha256':record['body_sha256'],
                    'start_line':start_line,'end_line':end_line,
                    'chunks':[{'start_line':start_line,'end_line':end_line,
                               'lines':selected}],
                    'snapshot_at':manifest.get('generated_at')}
    def _empty_result(self,path,start_line):
        return {'path':path,'source_host':self.host,
                'body_sha256':self.by_path[path]['body_sha256'],
                'start_line':start_line,'end_line':start_line,
                'chunks':[{'start_line':start_line,'end_line':start_line,
                           'lines':[]}],
                'snapshot_at':self.manifest.get('generated_at')}
    def _load_chunk(self,chunk):
        """Fetch, verify and decode one chunk; integrity failures are loud."""
        packed=self.store.get(chunk['key'],chunk['packed_bytes']+1)
        if len(packed)!=chunk['packed_bytes'] or \
                digest(packed)!=chunk['packed_sha256']:
            raise RuntimeError('Historical oversize chunk integrity failure')
        decoder=zlib.decompressobj()
        raw=decoder.decompress(packed,chunk['raw_bytes']+1)
        if (not decoder.eof or len(raw)!=chunk['raw_bytes'] or
                decoder.unused_data or digest(raw)!=chunk['sha256']):
            raise RuntimeError('Historical oversize chunk integrity failure')
        try:payload=raw.decode('utf-8')
        except UnicodeDecodeError:
            raise RuntimeError('Historical oversize chunk integrity failure')
        return payload
    def _verified_chunks(self,record,deadline):
        """Yield verified chunk texts in order under one shared deadline."""
        for chunk in record['chunks']:
            if time.monotonic()>=deadline:
                raise TimeoutError('Historical oversize scan deadline')
            yield self._load_chunk(chunk)
    def scan_lines(self,record,deadline):
        """Stream (line_number,text) for one oversize body. Line semantics
        match the shard reader exactly, including a '\r\n' terminator that
        was byte-split across two chunks mid-piece."""
        line=1;buffer='';cr=False
        for text in self._verified_chunks(record,deadline):
            for piece in text.splitlines(keepends=True):
                if cr:
                    cr=False
                    yield line,buffer
                    line+=1;buffer=''
                    if piece[:1]=='\n':
                        if not piece[1:]:continue
                        piece=piece[1:]
                if piece.endswith('\r'):
                    # A lone '\r' terminator can pair with a '\n' at the
                    # very start of the next piece (or next chunk); defer.
                    buffer+=piece[:-1];cr=True;continue
                terminator=_terminator_length(piece)
                if terminator:
                    full=buffer+piece
                    yield line,full[:len(full)-terminator]
                    line+=1;buffer=''
                else:
                    buffer+=piece
        if cr:
            # The trailing '\r' really was the final terminator.
            yield line,buffer
    def first_match(self,record,deadline,needle=None,case_sensitive=True,
                    regex_pair=None):
        """Find the first matching line of one oversize record.

        Literal mode returns the first line containing the needle
        (case-folded unless sensitive), exactly like the shard reader.
        Regex mode mirrors Reader.query: per-line grep semantics first, a
        whole-body search only when the pattern can span newlines. Returns
        (line_number, column, matched_text) or None. Raises TimeoutError
        when the shared deadline expires.
        """
        if regex_pair is not None:
            if regex_can_span_newlines(regex_pair[0].pattern) is not None:
                body=''.join(self._verified_chunks(record,deadline))
                found=regex_match_positions(regex_pair[0],regex_pair[1],
                                            body,deadline)
                return None if found is None else found
            std,hard=regex_pair
            for line_number,line in self.scan_lines(record,deadline):
                if time.monotonic()>=deadline:
                    raise TimeoutError('Historical oversize scan deadline')
                match=hard.search(line,timeout=max(deadline-time.monotonic(),1e-9)) \
                    if hard is not None else std.search(line)
                if match is not None:
                    return line_number,match.start(),line
            return None
        for line_number,line in self.scan_lines(record,deadline):
            hay=line if case_sensitive else line.casefold()
            probe=needle if case_sensitive else needle.casefold()
            if probe in hay:return line_number,0,line
        return None
