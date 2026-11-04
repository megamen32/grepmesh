"""Numeric profiler tests only: no real source, cloud, credentials or FTS."""
import gzip,json,os,tempfile,unittest
from pathlib import Path
from unittest.mock import Mock, patch
import native100_profile16 as p
from cloud_index import document,digest,json_bytes
from native100_bounded_resume import Limits
from test_native100_bounded_resume import make_fixture,engine

def part_data(n,count=5):
    raw=b''.join(json_bytes([n*100+i,'/home/synthetic/profile-%06d.txt'%(n*100+i),'needle text %06d'%(n*100+i)])+b'\n' for i in range(1,count+1))
    packed=gzip.compress(raw,mtime=0)
    return packed,{'sha256':digest(packed),'compressed_bytes':len(packed),'rows':count,'last_rowid':n*100+count}

class NumericProfileTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir='/run/user/1000',prefix='grepmesh-profile-unit-')
        self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name);self.root.chmod(0o700)
    def commit(self,n=1,ledger=None):
        ledger=ledger or p.Ledger(self.root,'a'*64);packed,part=part_data(n);model=p.Model(ledger.model.state)
        path,stats=p.profile_part(packed,part,n,model,self.root);ledger.save(part,path,model,stats)
        return ledger,part,path
    def test_numeric_profile_contains_no_paths_or_texts(self):
        ledger,part,path=self.commit();raw=gzip.decompress(path.read_bytes())
        self.assertEqual(len(raw),5*p.RECORD.size);self.assertNotIn(b'/home',raw);self.assertNotIn(b'needle',raw)
        self.assertEqual(sum(b['rows'] for b in ledger.model.state['partitions']),5)
        self.assertNotIn('/home',ledger.head.read_text())
    def test_source_size_sha_and_rows_fail_closed(self):
        packed,part=part_data(1)
        for raw in (packed[:-1],packed+b'x',b'torn'):
            with self.subTest(raw=len(raw)),self.assertRaises(ValueError):p.profile_part(raw,part,1,p.Model(),self.root)
        with self.assertRaises(ValueError):p.profile_part(packed,dict(part,rows=6),1,p.Model(),self.root)
        self.assertFalse((self.root/'head.json').exists())
    def test_torn_or_changed_checkpoint_and_numeric_payload_rejected(self):
        ledger,part,path=self.commit();original=path.read_bytes();path.write_bytes(b'bad')
        with self.assertRaises(ValueError):p.Ledger(self.root,'a'*64).restore([part])
        path.write_bytes(original);q=p.Ledger(self.root,'a'*64);q.restore([part]);self.assertEqual(q.cursor,1)
        h=json.loads(q.head.read_text());h['cursor']=2;q.head.write_bytes(json_bytes(h))
        with self.assertRaises(ValueError):p.Ledger(self.root,'a'*64).restore([part])
    def test_exact_policy_matches_production_queues_without_building_fts(self):
        base=self.root/'fixture';base.mkdir(mode=0o700)
        lim=Limits(batch_parts=2,pending_documents=7,pending_body_bytes=1024,max_sqlite_bytes=64*1024)
        make_fixture(base,parts=2,rows=2,limits=lim);e,_,_=engine(base);m=p.Model(limits=lim);events=[]
        def flush(pid,reason='batch_tail'):
            if not e.fill.counts[pid]:return
            docs=e.fill.take(pid);events.append((pid,reason,len(docs)))
        e._flush_docs=flush
        for n in range(1,301):
            path='/home/synthetic/row-%05d.txt'%n;body=('x%d'%n)*20;doc=document(path,body);frame=p.FilledQueues.encode(doc)
            pid=p.hashlib.sha256(path.encode()).digest()[0]%16
            m.add(pid,n,len(body.encode()),len(path),len(path),200,len(frame),40,100);e._add_doc(pid,path,body,len(body.encode()))
            self.assertEqual([q['n'] for q in m.state['queue']],e.fill.counts)
            self.assertEqual([q['body'] for q in m.state['queue']],e.fill.body_bytes)
            self.assertEqual([q['frames'] for q in m.state['queue']],e.fill.file_bytes)
        m.batch_end()
        for pid in range(16):e._flush_docs(pid)
        self.assertEqual([b['packing_groups'] for b in m.state['partitions']],[sum(1 for x in events if x[0]==pid) for pid in range(16)])
        self.assertLessEqual(m.state['peak_queue_bytes'],lim.max_sqlite_bytes//4)
    def test_missing_mandatory_cache_does_not_fall_back_to_cloud(self):
        store=p.ReadOnlyStore();ordinal='000425';size,sha=p.CACHED[ordinal]
        part={'key':p.native.PREFIX+'/parts/'+ordinal+'-'+sha+'.jsonl.gz','compressed_bytes':size,'sha256':sha}
        with patch.object(p.native,'ROOT',self.root),patch.object(p.native.CloudStore,'get',side_effect=AssertionError('network')):
            with self.assertRaises(FileNotFoundError):store.source_get(part)
    def test_readonly_store_refuses_any_cloud_write(self):
        with self.assertRaises(PermissionError):p.ReadOnlyStore().put('anything',b'x')
    def test_metadata_cap_checked_before_next_read(self):
        packed,part=part_data(1)
        reserve=part['rows']*p.RECORD.size+2*p.MIB
        ledger=p.Ledger(self.root,'a'*64,cap=reserve-1)
        source=Mock(side_effect=AssertionError('source must not be called'))
        with patch.object(p,'profile_part',side_effect=AssertionError('no numeric write')) as numeric:
            with self.assertRaisesRegex(ValueError,'Durable numeric metadata byte cap'):
                p.profile_next_part(ledger,part,source)
        source.assert_not_called();numeric.assert_not_called()
        self.assertEqual(ledger.cursor,0);self.assertFalse(ledger.head.exists())

    def test_cap_accounts_existing_metadata_before_any_source_read(self):
        _,part=part_data(1);reserve=part['rows']*p.RECORD.size+2*p.MIB
        retained=self.root/'retained.metadata';retained.write_bytes(b'x'*64);retained.chmod(0o600)
        ledger=p.Ledger(self.root,'a'*64,cap=reserve+63)
        source=Mock(side_effect=AssertionError('no cache or cloud source call'))
        with self.assertRaisesRegex(ValueError,'Durable numeric metadata byte cap'):
            p.profile_next_part(ledger,part,source)
        source.assert_not_called();self.assertEqual(retained.read_bytes(),b'x'*64)
        self.assertFalse(ledger.head.exists())

    def test_admitted_part_orders_cap_read_numeric_and_commit(self):
        packed,part=part_data(1);events=[]
        def event(stage,info):events.append(stage)
        ledger=p.Ledger(self.root,'a'*64,on_event=event)
        original=ledger.check_space
        def check(reserve=0):
            events.append('cap');return original(reserve)
        def source(descriptor):
            self.assertEqual(events,['cap'])
            self.assertEqual(descriptor,part);events.append('source_read')
            return packed,'synthetic'
        with patch.object(ledger,'check_space',side_effect=check):
            origin=p.profile_next_part(ledger,part,source,last_part=True,on_event=event)
        self.assertEqual(origin,'synthetic')
        self.assertEqual(events,['cap','source_read','after_numeric','cap',
                                 'after_commit','before_head','after_head','after_checkpoint'])
        self.assertEqual(ledger.cursor,1)
        self.assertEqual(ledger.model.state['rows'],5)

    def test_invalid_row_admission_never_calls_source(self):
        _,part=part_data(1);ledger=p.Ledger(self.root,'a'*64)
        source=Mock(side_effect=AssertionError('source must not be called'))
        for rows in (0,-1,True,'5'):
            with self.subTest(rows=rows),self.assertRaises(ValueError):
                p.profile_next_part(ledger,dict(part,rows=rows),source)
        source.assert_not_called();self.assertFalse(ledger.head.exists())
    def test_existing_identical_uncommitted_numeric_replays_without_overwrite(self):
        packed,part=part_data(1)
        first=p.Model();path,stats=p.profile_part(packed,part,1,first,self.root)
        initial=path.read_bytes()
        replay=p.Model();same,new_stats=p.profile_part(packed,part,1,replay,self.root)
        self.assertEqual(same,path);self.assertEqual(path.read_bytes(),initial)
        self.assertEqual(stats,new_stats)
        self.assertEqual(first.state,replay.state)

    def test_changed_uncommitted_numeric_fails_closed(self):
        packed,part=part_data(1);path,_=p.profile_part(packed,part,1,p.Model(),self.root)
        old=path.read_bytes()
        changed=gzip.compress(b''.join(json_bytes([101+i,
            '/home/synthetic/profile-%06d.txt'%(101+i),'CHANGED %06d'%(101+i)])+
            b'\n' for i in range(5)),mtime=0)
        different=dict(part,sha256=digest(changed),compressed_bytes=len(changed))
        with self.assertRaisesRegex(RuntimeError,'Unconfirmed numeric part differs'):
            p.profile_part(changed,different,1,p.Model(),self.root)
        self.assertEqual(path.read_bytes(),old)

    def test_orphan_commit_before_first_head_is_replayed_not_skipped(self):
        packed,part=part_data(1);first=p.Model()
        path,stats=p.profile_part(packed,part,1,first,self.root)
        initial=path.read_bytes();ledger=p.Ledger(self.root,'a'*64)
        original=p.atomic
        def fail_before_head(target,raw,**kwargs):
            if Path(target).name=='head.json':
                raise RuntimeError('simulated lost first head publication')
            return original(target,raw,**kwargs)
        with patch.object(p,'atomic',side_effect=fail_before_head):
            with self.assertRaisesRegex(RuntimeError,'simulated lost first head'):
                ledger.save(part,path,first,stats)
        self.assertFalse((self.root/'head.json').exists())
        resumed=p.Ledger(self.root,'a'*64);resumed.restore([part])
        self.assertEqual(resumed.cursor,0)
        replay=p.Model(resumed.model.state)
        same,new_stats=p.profile_part(packed,part,1,replay,self.root)
        resumed.save(part,same,replay,new_stats)
        self.assertEqual(resumed.cursor,1)
        self.assertEqual(path.read_bytes(),initial)
        verified=p.Ledger(self.root,'a'*64);verified.restore([part])
        self.assertEqual(verified.cursor,1)


    def test_bad_crc_with_matching_descriptor_reaches_gzip_trailer(self):
        packed,part=part_data(1)
        broken=bytearray(packed);broken[-8]^=1;broken=bytes(broken)
        descriptor=dict(part,compressed_bytes=len(broken),sha256=digest(broken))
        # The descriptor deliberately matches; a SHA/length ValueError is NOT a pass.
        with self.assertRaisesRegex(gzip.BadGzipFile,'CRC check failed'):
            p.profile_part(broken,descriptor,1,p.Model(),self.root)
        self.assertFalse(list(self.root.glob('*.numeric.gz')))
        self.assertFalse(list(self.root.glob('*.pending-*')))
        self.assertFalse((self.root/'head.json').exists())

    def test_bad_isize_with_matching_descriptor_reaches_gzip_trailer(self):
        packed,part=part_data(1)
        broken=bytearray(packed);broken[-4]^=1;broken=bytes(broken)
        descriptor=dict(part,compressed_bytes=len(broken),sha256=digest(broken))
        with self.assertRaisesRegex(gzip.BadGzipFile,'Incorrect length of data produced'):
            p.profile_part(broken,descriptor,1,p.Model(),self.root)
        self.assertFalse(list(self.root.glob('*.numeric.gz')))
        self.assertFalse(list(self.root.glob('*.pending-*')))
        self.assertFalse((self.root/'head.json').exists())

    def test_numeric_gzip_has_no_filename_or_time_and_replays_identically(self):
        packed,part=part_data(1)
        with patch.object(p.os,'getpid',return_value=111111):
            path,stats=p.profile_part(packed,part,1,p.Model(),self.root)
        before=path.read_bytes()
        self.assertEqual(before[:3],b'\x1f\x8b\x08')
        self.assertEqual(before[3]&8,0,'gzip must not contain FNAME')
        self.assertEqual(before[4:8],b'\x00'*4,'gzip MTIME must be zero')
        with patch.object(p.os,'getpid',return_value=222222):
            same,again=p.profile_part(packed,part,1,p.Model(),self.root)
        self.assertEqual(same,path);self.assertEqual(before,path.read_bytes())
        self.assertEqual(stats,again);self.assertEqual(path.stat().st_nlink,1)

    def test_numeric_crash_barrier_exposes_durable_single_link_file(self):
        packed,part=part_data(1);seen=[]
        def observe(stage,info):
            self.assertEqual(stage,'after_numeric');self.assertEqual(info,{'ordinal':1})
            path=self.root/'part-000001.numeric.gz'
            self.assertEqual(path.stat().st_nlink,1)
            self.assertFalse(list(self.root.glob('*.pending-*')))
            self.assertFalse((self.root/'head.json').exists())
            seen.append(stage)
        p.profile_part(packed,part,1,p.Model(),self.root,on_event=observe)
        self.assertEqual(seen,['after_numeric'])

if __name__=='__main__':unittest.main()
