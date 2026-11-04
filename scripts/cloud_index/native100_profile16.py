#!/usr/bin/env python3
"""One serialized read-only source pass; numeric metadata only, no FTS/cloud writes."""
from __future__ import annotations
import argparse, datetime as dt, fcntl, gzip, hashlib, io, json, math, os
from pathlib import Path
import resource, signal, stat, struct, subprocess, sys, time, zlib
import publish_native100_rolling_private as native
from cloud_index import MAX_DOCUMENT, MIB, digest, document, json_bytes
from native100_bounded_resume import Limits
from native100_shard_fill import FilledQueues, PACKING_POLICY
from native100_profile16_model import Model as NumericModel
BASE='ee35b97d39f363ff9b64083b7172d2839ab623dd'
SOURCE_SHA='96f1736a8cd30e9a9a7427255c995d7562fdc9536ec5272c78db509f519409ce'
DEST='yadisk:Backups/GrepMesh/cloud-index-v1'
SCHEMA='grepmesh-numeric-profile16-v1'
STATE_ROOT=Path('/home/roomhacker/.local/state/grepmesh-native100-profile16-20261103')
METADATA_CAP=160*MIB
PART_DEADLINE=900
# Numeric only: partition,rowid,body/path/fold/line/frame/zlib/trigram sizes.
RECORD=struct.Struct('<BQIIIIIII')
BLOCK=256*1024
CACHED={'000424':(12086993,'cfe29a793b036ed4abdc00af2e3e1691aaea85abe801bd11445868c28158ae98'),
        '000425':(5746993,'2afeae188593ca70e437df139a375ce8071c93eda504a0e9d1ebc8ef93db36dd')}
UNITS=('grepmesh-native100-private-search-resume-20261101.service',
       'grepmesh-remote44-oversize-offload-v3-20261103.service',
       'grepmesh-remote44-immutable-restore.service')

def utc():return dt.datetime.now(dt.timezone.utc).isoformat()

def private_read(path,cap):
    path=Path(path);info=path.lstat()
    if not stat.S_ISREG(info.st_mode):raise RuntimeError('Not a regular private metadata file')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
    with os.fdopen(fd,'rb') as f:
        s=os.fstat(f.fileno())
        if s.st_uid!=os.getuid() or s.st_mode&0o077 or s.st_nlink!=1:raise RuntimeError('Unsafe private metadata inode')
        raw=f.read(cap+1)
        if len(raw)>cap:raise ValueError('Private metadata exceeds bound')
    return raw

def dirsync(path):
    fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:os.fsync(fd)
    finally:os.close(fd)

def atomic(path,raw,*,replace=False):
    path=Path(path);tmp=path.with_name(path.name+'.pending-'+str(os.getpid()))
    fd=os.open(tmp,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
    if replace:
        if path.exists():private_read(path,MIB)
        os.replace(tmp,path)
    else:
        os.link(tmp,path,follow_symlinks=False);tmp.unlink()
    dirsync(path.parent)

class Model(NumericModel):
    """Compatibility adapter: keep the existing runtime Limits and API."""
    def __init__(self,state=None,*,limits=None):
        super().__init__(state,limits=limits or Limits())


class Ledger:
    """Durable hashed per-source-part numeric metadata and commit chain."""
    def __init__(self,root,source_sha,cap=METADATA_CAP,*,on_event=None):
        self.root=Path(root);self.source_sha=source_sha;self.cap=cap
        self.root.mkdir(mode=0o700,parents=False,exist_ok=True);st=self.root.lstat()
        if not stat.S_ISDIR(st.st_mode) or st.st_uid!=os.getuid() or st.st_mode&0o077:raise RuntimeError('Unsafe profile metadata root')
        self.head=self.root/'head.json';self.ref=None;self.cursor=0;self.model=Model()
        # Explicit test observation only; main() does not pass a callback.
        # No environment variable, signal handler or alternate runtime mode.
        self.on_event=on_event
    def _event(self,stage,ordinal):
        if self.on_event is not None:self.on_event(stage,{'ordinal':ordinal})
    def check_space(self,reserve=0):
        used=sum(p.lstat().st_size for p in self.root.iterdir() if p.is_file())
        if used+reserve>self.cap:raise ValueError('Durable numeric metadata byte cap')
        return used
    def save(self,part,numeric_path,model,part_stats):
        ordinal=self.cursor+1;raw=private_read(numeric_path,16*MIB)
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as f:
            n=0;h=hashlib.sha256()
            while True:
                chunk=f.read(BLOCK)
                if not chunk:break
                h.update(chunk);n+=len(chunk)
                if n>part['rows']*RECORD.size:raise ValueError('Numeric metadata grew')
        if n!=part['rows']*RECORD.size:raise ValueError('Numeric metadata row count mismatch')
        value={'schema':SCHEMA,'source':self.source_sha,'packing_policy':PACKING_POLICY,
            'cursor':ordinal,'previous':self.ref,'source_part_sha256':part['sha256'],'limits':vars(model.limits),
            'numeric_metadata':{'file':numeric_path.name,'bytes':len(raw),'sha256':digest(raw),
                'uncompressed_sha256':h.hexdigest(),'rows':part['rows'],'record_bytes':RECORD.size},
            'part':part_stats,'state':model.state}
        blob=json_bytes(value);name='commit-%06d-%s.json'%(ordinal,digest(blob));self.check_space(len(blob)+65536)
        # A crash may occur after publishing the immutable commit but before
        # advancing head.json. Replay must accept identical content only.
        commit_path=self.root/name
        if commit_path.exists() or commit_path.is_symlink():
            if private_read(commit_path,MIB)!=blob:
                raise RuntimeError('Unconfirmed numeric commit differs; no overwrite')
        else:
            atomic(commit_path,blob)
        self._event('after_commit',ordinal)
        ref={'file':name,'sha256':digest(blob)}
        self._event('before_head',ordinal)
        atomic(self.head,json_bytes({'schema':SCHEMA,'source':self.source_sha,'cursor':ordinal,'commit':ref}),replace=True)
        self._event('after_head',ordinal)
        self.cursor=ordinal;self.ref=ref;self.model=model
        self._event('after_checkpoint',ordinal)
        return value
    def restore(self,parts):
        if not self.head.exists():
            # SIGKILL between the immutable first commit and head publication:
            # this is NOT a confirmed part. Inspect the sole orphan under the
            # original lock, then replay part 1 and compare the commit bytes.
            orphans=list(self.root.glob('commit-*.json'))
            if orphans:
                if len(orphans)!=1 or not parts:
                    raise RuntimeError('Ambiguous orphan commits; no takeover')
                orphan=orphans[0];raw=private_read(orphan,MIB)
                if orphan.name!='commit-000001-'+digest(raw)+'.json':
                    raise RuntimeError('Orphan commit identity mismatch')
                c=json.loads(raw);meta=c.get('numeric_metadata')
                if (c.get('schema')!=SCHEMA or c.get('source')!=self.source_sha
                    or c.get('cursor')!=1 or c.get('previous') is not None
                    or c.get('packing_policy')!=PACKING_POLICY
                    or c.get('limits')!=vars(Limits())
                    or c.get('source_part_sha256')!=parts[0]['sha256']
                    or not isinstance(meta,dict)
                    or meta.get('file')!='part-000001.numeric.gz'
                    or meta.get('rows')!=parts[0]['rows']
                    or meta.get('record_bytes')!=RECORD.size):
                    raise RuntimeError('Orphan commit provenance mismatch')
                candidate=private_read(self.root/meta['file'],16*MIB)
                if len(candidate)!=meta.get('bytes') or digest(candidate)!=meta.get('sha256'):
                    raise RuntimeError('Orphan numeric metadata mismatch')
            return
        h=json.loads(private_read(self.head,4096))
        if h.get('schema')!=SCHEMA or h.get('source')!=self.source_sha or not 1<=h.get('cursor',0)<=len(parts):raise ValueError('Profile head identity mismatch')
        ref=h['commit'];expected=h['cursor'];latest=None
        while ref is not None:
            if not isinstance(ref,dict) or set(ref)!=set(('file','sha256')):raise ValueError('Malformed commit reference')
            name='commit-%06d-%s.json'%(expected,ref['sha256'])
            if ref['file']!=name:raise ValueError('Profile commit reference is not canonical')
            raw=private_read(self.root/name,MIB)
            if digest(raw)!=ref['sha256']:raise ValueError('Profile commit digest mismatch')
            c=json.loads(raw);p=parts[expected-1]
            if (c.get('schema')!=SCHEMA or c.get('source')!=self.source_sha or c.get('cursor')!=expected
                or c.get('packing_policy')!=PACKING_POLICY or c.get('limits')!=vars(Limits())
                or c.get('source_part_sha256')!=p['sha256']):raise ValueError('Profile part/commit mismatch')
            meta=c['numeric_metadata'];filename='part-%06d.numeric.gz'%expected
            if meta['file']!=filename or meta['rows']!=p['rows'] or meta['record_bytes']!=RECORD.size:raise ValueError('Numeric profile provenance mismatch')
            data=private_read(self.root/filename,16*MIB)
            if len(data)!=meta['bytes'] or digest(data)!=meta['sha256']:raise ValueError('Numeric profile missing/changed')
            if latest is None:latest=c
            expected-=1;ref=c['previous']
        if expected!=0:raise ValueError('Incomplete profile chain')
        if latest['state']['rows']!=sum(p['rows'] for p in parts[:h['cursor']]):raise ValueError('Profile cursor cannot skip source rows')
        if latest['state']['last_rowid']!=parts[h['cursor']-1]['last_rowid']:raise ValueError('Profile source highwater mismatch')
        self.model=Model(latest['state']);self.cursor=h['cursor'];self.ref=h['commit']

class ReadOnlyStore(native.VerifiedImmutableReuseStore):
    def _load_verified_inventory(self):return {}  # No FTS output inventory needed.
    def put(self,*args,**kwargs):raise PermissionError('Cloud writes forbidden in metadata pass')
    def source_get(self,part):
        key=part['key'];ordinal=key.rsplit('/',1)[-1].split('-',1)[0]
        path=native.ROOT/'verified-source-cache'/key.rsplit('/',1)[-1]
        if ordinal in CACHED:
            size,sha=CACHED[ordinal]
            if part['compressed_bytes']!=size or part['sha256']!=sha:raise ValueError('Pinned mandatory cache descriptor changed')
            raw=private_read(path,64*MIB)
            if len(raw)!=size or digest(raw)!=sha:raise ValueError('Mandatory cache checksum failed')
            return raw,'verified_local_cache'
        if path.exists() or path.is_symlink():
            raw=private_read(path,64*MIB)
            if len(raw)!=part['compressed_bytes'] or digest(raw)!=part['sha256']:raise ValueError('Existing cached source mismatch; no download fallback')
            return raw,'verified_local_cache'
        # Only a known timeout may use the reviewed ranged fallback. A nonzero
        # or denied ordinary read is never retried through another route here.
        try:raw=native.CloudStore.get(self,key,part['compressed_bytes'])
        except subprocess.TimeoutExpired:raw=self._slow_source_part_get(key,part['compressed_bytes'])
        return raw,'authorized_cloud_read'

def assert_quiet():
    result={}
    for name in UNITS:
        r=subprocess.run(['systemctl','--user','show',name,'-p','ActiveState','-p','MainPID','-p','ControlPID'],text=True,capture_output=True,timeout=10,check=True)
        info=dict(line.split('=',1) for line in r.stdout.splitlines() if '=' in line)
        if info.get('ActiveState') not in ('failed','inactive') or info.get('MainPID')!='0' or info.get('ControlPID')!='0':raise RuntimeError('Conflicting or unknown existing operation')
        result[name]=info
    return result

def profile_part(packed,part,ordinal,model,root,*,on_event=None):
    if len(packed)!=part['compressed_bytes'] or digest(packed)!=part['sha256']:raise ValueError('Full source SHA/length mismatch')
    final=Path(root)/('part-%06d.numeric.gz'%ordinal);tmp=final.with_name(final.name+'.pending-'+str(os.getpid()))
    # A SIGKILL can land after the complete numeric part is linked but before
    # the durable head advances. Recreate that *uncommitted* part and compare
    # the exact deterministic gzip bytes; never overwrite unknown data.
    previous=private_read(final,16*MIB) if final.exists() or final.is_symlink() else None
    fd=os.open(tmp,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    count=0;decoded=0;before=[b['rows'] for b in model.state['partitions']]
    try:
        with os.fdopen(fd,'wb') as sink:
            # Fix BOTH variable gzip-header inputs. Never embed the PID-scoped
            # scratch filename, even when fileobj.name is a string path.
            with gzip.GzipFile(filename='',fileobj=sink,mode='wb',compresslevel=1,mtime=0) as numeric:
                with gzip.GzipFile(fileobj=io.BytesIO(packed)) as source:
                    while True:
                        line=source.readline(model.limits.max_source_line+1)
                        if not line:break
                        if len(line)>model.limits.max_source_line or not line.endswith(b'\n'):raise ValueError('Source JSONL exceeds unchanged admission')
                        row=json.loads(line)
                        if not isinstance(row,list) or len(row)!=3:raise ValueError('Malformed source row')
                        rid,path,body=row
                        if (type(rid) is not int or rid<=model.state['last_rowid'] or not isinstance(path,str)
                            or not path.startswith('/') or '..' in Path(path).parts or '\x00' in path
                            or os.path.normpath(path)!=path or not isinstance(body,str)):raise ValueError('Source identity/path/body invalid')
                        p=path.encode('utf8');fold=path.casefold().encode('utf8')
                        if len(p)>4096:raise ValueError('Source path exceeds unchanged admission')
                        data=body.encode('utf8');size=len(data);pid=hashlib.sha256(p).digest()[0]%16
                        if size<=MAX_DOCUMENT:
                            frame=FilledQueues.encode(document(path,body,0,0))
                            frame_size=len(frame);comp_size=len(zlib.compress(data,3))
                            trigram_upper=max(0,len(path)-2)+max(0,len(body)-2);del frame
                        else:frame_size=comp_size=trigram_upper=0
                        values=(pid,rid,size,len(p),len(fold),len(line),frame_size,comp_size,trigram_upper)
                        numeric.write(RECORD.pack(*values))
                        model.add(pid,rid,size,len(p),len(fold),len(line),frame_size,comp_size,trigram_upper)
                        count+=1;decoded+=len(line);del line,row,path,body,data,p,fold
            sink.flush();os.fsync(sink.fileno())
        if count!=part['rows'] or model.state['last_rowid']!=part['last_rowid']:raise ValueError('Source EOF/rows/highwater mismatch')
        if previous is None:
            os.link(tmp,final,follow_symlinks=False)
        elif private_read(tmp,16*MIB)!=previous:
            raise RuntimeError('Unconfirmed numeric part differs; no overwrite')
        # Publish a private single-link inode before exposing a crash barrier.
        # The durable head has not advanced; replay verifies the same bytes.
        tmp.unlink()
        dirsync(final.parent)
        if on_event is not None:on_event('after_numeric',{'ordinal':ordinal})
        return final,{'ordinal':ordinal,'rows':count,'decoded_bytes':decoded,
            'partition_rows':[b['rows']-before[i] for i,b in enumerate(model.state['partitions'])],
            'source_sha256':part['sha256'],'sha_length_gzip_crc_eof':'PASS'}
    finally:
        # Only our reproducible, PID-scoped metadata scratch; never source,
        # remote, cached parts, partial archives, or committed numeric files.
        tmp.unlink(missing_ok=True)

def profile_next_part(ledger,part,source_get,*,last_part=False,on_event=None):
    """The single capacity-before-read boundary, also used by main().

    source_get is injected, but has no alternative/default transport here.
    A denied reserve must raise before cache/body reads or numeric writes.
    The reservation and runtime limits are unchanged from the reviewed source.
    """
    ordinal=ledger.cursor+1
    if type(part.get('rows')) is not int or part['rows']<1:
        raise ValueError('Invalid source row count for metadata admission')
    ledger.check_space(part['rows']*RECORD.size+2*MIB)
    packed,origin=source_get(part)
    next_model=Model(ledger.model.state)
    path,stats=profile_part(packed,part,ordinal,next_model,ledger.root,on_event=on_event)
    del packed
    if ordinal%next_model.limits.batch_parts==0 or last_part:next_model.batch_end()
    stats['origin']=origin
    ledger.save(part,path,next_model,stats)
    return origin


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply',action='store_true');parser.add_argument('--resume',action='store_true')
    parser.add_argument('--seconds',type=int,default=7200);args=parser.parse_args()
    if not args.apply or not 900<=args.seconds<=7200:raise SystemExit('Explicit bounded apply required')
    if os.getuid()!=1000 or os.environ.get('GREPMESH_CLOUD_GATEWAY')!='local':raise SystemExit('Existing local roomhacker gateway required')
    os.umask(0o077);before=assert_quiet();lock=native.ROOT/'private-build.lock';st=lock.lstat()
    if not stat.S_ISREG(st.st_mode) or st.st_uid!=1000 or st.st_mode&0o077 or st.st_nlink!=1:raise RuntimeError('Original builder lock inode unsafe')
    fd=os.open(lock,os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
    fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);assert_quiet()
    started=time.monotonic();deadline=started+args.seconds;ledger=Ledger(STATE_ROOT,SOURCE_SHA)
    status_path=ledger.root/'operation.json'
    if status_path.exists():
        old=json.loads(private_read(status_path,MIB))
        if not args.resume:
            raise RuntimeError('Existing profile requires explicit resume')
        if (old.get('schema')!=SCHEMA or
            old.get('source_checkpoint_sha256')!=SOURCE_SHA or
            old.get('packing_policy')!=PACKING_POLICY or
            old.get('base')!=BASE):
            raise RuntimeError('Previous profile identity differs; no takeover')
        if old.get('phase')=='completed':
            raise RuntimeError('Profile already completed')
        if old.get('phase') not in ('starting','reading_source_checkpoint','running','paused_budget','failed'):
            raise RuntimeError('Unknown profile outcome; no replay')
        # This process already holds the ORIGINAL exclusive flock and confirmed
        # original builder/transfer/restore quiet. SIGKILL can leave status as
        # "running"; the verified durable head (not operation.json) determines
        # which source parts may be skipped.
    status={'schema':SCHEMA,'utc_started':utc(),'pid':os.getpid(),'phase':'starting',
        'source_checkpoint_sha256':SOURCE_SHA,'base':BASE,'packing_policy':PACKING_POLICY,
        'original_lock_exclusive':True,'guard_units':before,'max_wall_seconds':args.seconds,
        'per_part_deadline_seconds':PART_DEADLINE,'persistent_metadata_cap_bytes':METADATA_CAP,
        'numeric_record_bytes':RECORD.size,'cloud_writes':0,'FTS_created':False,'production_builder_started':False,
        'partial_archive_accessed':False,'cursor':0,'rows':0,'local_cache_parts':[],'cloud_parts_read':0}
    def update(**extra):
        status.update(extra);status['updated_utc']=utc();status['elapsed_seconds']=round(time.monotonic()-started,3)
        status['peak_rss_kib']=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        atomic(status_path,json_bytes(status),replace=True)
    def deadline_signal(signum,frame):raise TimeoutError('Bounded profile deadline/termination')
    signal.signal(signal.SIGALRM,deadline_signal);signal.signal(signal.SIGTERM,deadline_signal)
    try:
        update(phase='reading_source_checkpoint');store=ReadOnlyStore()
        if store.prefix!=DEST:raise RuntimeError('Private destination differs from approved scope')
        cp=ledger.root/'source-checkpoint.json'
        if cp.exists():raw=private_read(cp,16*MIB)
        else:
            raw=native.CloudStore.get(store,native.PREFIX+'/progress.json',16*MIB)
            if digest(raw)!=SOURCE_SHA:raise ValueError('Source checkpoint changed')
            atomic(cp,raw)
        verification=json.loads(native.SOURCE_VERIFICATION.read_text())
        progress=store.bind_verified_source_checkpoint(raw,verification);audit=json.loads(native.AUDIT.read_text())
        if audit.get('snapshot')!=native.SNAPSHOT or audit.get('cloud_parts_fully_replayed')!=443:raise ValueError('Original independent audit unavailable')
        parts=progress['chunks']
        if args.resume:ledger.restore(parts)
        elif ledger.head.exists():raise RuntimeError('Existing durable profile requires explicit resume')
        # Inode preflight only; cached bytes are verified at their natural position.
        for ordinal,(size,sha) in CACHED.items():
            p=native.ROOT/'verified-source-cache'/(ordinal+'-'+sha+'.jsonl.gz');s=p.lstat()
            if not stat.S_ISREG(s.st_mode) or s.st_uid!=1000 or s.st_mode&0o077 or s.st_nlink!=1 or s.st_size!=size:raise ValueError('Mandatory 424/425 cache absent or changed; never download')
        update(phase='running',cursor=ledger.cursor,rows=ledger.model.state['rows'],source_parts_total=len(parts))
        while ledger.cursor<len(parts):
            if deadline-time.monotonic()<PART_DEADLINE:update(phase='paused_budget');return 0
            ordinal=ledger.cursor+1;part=parts[ledger.cursor]
            update(current_part=ordinal,part_phase='reading',part_started_utc=utc());signal.alarm(PART_DEADLINE)
            origin=profile_next_part(ledger,part,store.source_get,last_part=ordinal==len(parts))
            next_model=ledger.model;signal.alarm(0)
            local=list(status['local_cache_parts'])
            if origin=='verified_local_cache':local.append(ordinal)
            update(phase='running',cursor=ledger.cursor,rows=next_model.state['rows'],
                cloud_parts_read=status['cloud_parts_read']+int(origin=='authorized_cloud_read'),local_cache_parts=local,
                part_phase='durably_committed',counts16=[b['searchable'] for b in next_model.state['partitions']],
                packing_groups16=[b['packing_groups'] for b in next_model.state['partitions']],
                metadata_bytes=ledger.check_space(),current_lower_bound_exceeds_limits=any(b['packing_groups']>500 for b in next_model.state['partitions']))
        s=ledger.model.state;b=s['partitions']
        if (s['rows']!=progress['rows'] or s['last_rowid']!=progress['last_rowid'] or s['body_bytes']!=audit['total_text_body_bytes']
            or sum(x['oversized'] for x in b)!=audit['oversize_documents']
            or [b[i]['searchable']+b[i+8]['searchable'] for i in range(8)]!=audit['bucket_documents_under_1mib']):raise ValueError('Full profile differs from independently verified audit')
        summary=ledger.model.summary(complete=True)
        summary.update(schema=SCHEMA,source_checkpoint_sha256=SOURCE_SHA,completed_utc=utc(),checkpoint=ledger.ref,
            numeric_metadata_schema=RECORD.format,no_raw_paths_or_bodies_persisted=True,source_parts=ledger.cursor,
            limits=vars(Limits()),metadata_bytes=ledger.check_space())
        atomic(ledger.root/'profile16.json',json_bytes(summary))
        update(phase='completed',profile16_sha256=digest(json_bytes(summary)),capacity_rejection_proved=summary['capacity_rejection_proved'],full_snapshot_capacity_accepted=False)
        print(json.dumps({k:status[k] for k in ('phase','pid','cursor','rows','cloud_parts_read','local_cache_parts','peak_rss_kib')},sort_keys=True));return 0
    except BaseException as exc:
        signal.alarm(0)
        # Never emit raw remote stderr, source paths or exception text.
        update(phase='failed',error_type=type(exc).__name__,failed_part=ledger.cursor+1,cursor=ledger.cursor,rows=ledger.model.state['rows'],unknown_cloud_write=False)
        print(json.dumps({'phase':'failed','error_type':type(exc).__name__,'cursor':ledger.cursor,'part':ledger.cursor+1}));return 1
    finally:signal.alarm(0);os.close(fd)

if __name__=='__main__':raise SystemExit(main())
