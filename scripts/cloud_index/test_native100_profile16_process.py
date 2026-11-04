"""Proposed process-control coverage; SOURCE ONLY, NOT EXECUTED.

This file describes real SIGKILL/new-process cases for later authorized review.
It is not a replacement route or permission to run a subset of the denied action.
All data would be synthetic, inside a new private test-owned temporary directory.
"""
import gzip
import hashlib
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import tempfile
import unittest

import native100_profile16 as p
from test_native100_profile16 import part_data


def worker(root,mode,stop_stage='',stop_ordinal=-1):
    root=Path(root)
    fixtures=[part_data(i) for i in range(1,4)]
    parts=[item[1] for item in fixtures]
    reads=[]

    def barrier(stage,info):
        if stage==stop_stage and info['ordinal']==stop_ordinal:
            print(json.dumps({'stage':stage,'ordinal':stop_ordinal,'pid':os.getpid()}),flush=True)
            sys.stdin.buffer.read(1)
            raise RuntimeError('Test parent must SIGKILL its own waiting child')

    ledger=p.Ledger(root,'a'*64,on_event=barrier)
    if mode=='resume':ledger.restore(parts)
    elif mode!='init':raise ValueError('Invalid synthetic worker mode')
    for n in range(ledger.cursor+1,4):
        packed,part=fixtures[n-1]
        def source(descriptor):
            if descriptor!=part:raise AssertionError('Wrong synthetic source descriptor')
            reads.append(n)
            return packed,'synthetic'
        # No ReadOnlyStore, rclone, real source path, credentials or main().
        p.profile_next_part(ledger,part,source,last_part=n==3,on_event=barrier)
    print(json.dumps({'reads':reads,'rows':ledger.model.state['rows'],
                      'cursor':ledger.cursor,'pid':os.getpid()}),flush=True)


def immutable_digests(root):
    return {f.name:hashlib.sha256(f.read_bytes()).hexdigest()
            for f in root.iterdir()
            if f.name.startswith('commit-') or f.name.endswith('.numeric.gz')}


class ProfileProcessTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir='/run/user/1000',prefix='grepmesh-profile-crash-')
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.root.chmod(0o700)

    def command(self,root,mode,stage='',ordinal=-1):
        return [sys.executable,'-B',str(Path(__file__).resolve()),'--profile16-worker',
                str(root),mode,stage,str(ordinal)]

    def run_child(self,root,mode):
        result=subprocess.run(self.command(root,mode),capture_output=True,text=True,timeout=30)
        self.assertEqual(result.returncode,0,result.stderr[-1000:])
        return json.loads(result.stdout.strip())

    def kill_at(self,root,stage,ordinal):
        proc=subprocess.Popen(self.command(root,'init',stage,ordinal),stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(proc.stdout,selectors.EVENT_READ)
                self.assertTrue(selector.select(20),'Synthetic child did not reach barrier')
                line=proc.stdout.readline()
            if not line:
                _,error=proc.communicate(timeout=5)
                self.fail('Synthetic worker exited before barrier: '+error[-1000:])
            self.assertEqual(json.loads(line),{'stage':stage,'ordinal':ordinal,'pid':proc.pid})
            # Only the Popen-owned child, never an existing service/PID/group.
            os.kill(proc.pid,signal.SIGKILL)
            proc.communicate(timeout=5)
            self.assertEqual(proc.returncode,-signal.SIGKILL)
            return proc.pid
        finally:
            if proc.poll() is None:proc.kill();proc.wait(timeout=5)
            for stream in (proc.stdin,proc.stdout,proc.stderr):
                if stream:stream.close()

    def assert_complete_numeric(self,root):
        actual=[]
        for n in range(1,4):
            raw=gzip.decompress((root/('part-%06d.numeric.gz'%n)).read_bytes())
            self.assertEqual(len(raw),5*p.RECORD.size)
            actual.extend(row[1] for row in p.RECORD.iter_unpack(raw))
        expected=[n*100+i for n in range(1,4) for i in range(1,6)]
        self.assertEqual(actual,expected)
        self.assertEqual(len(actual),len(set(actual)))
        self.assertEqual(json.loads((root/'head.json').read_text())['cursor'],3)

    def prove(self,stage,ordinal,confirmed):
        root=self.root/(stage+'-'+str(ordinal));root.mkdir(mode=0o700)
        killed=self.kill_at(root,stage,ordinal)
        # A durable numeric/commit file is not, by itself, a confirmed head.
        if confirmed:
            self.assertEqual(json.loads((root/'head.json').read_text())['cursor'],confirmed)
        else:self.assertFalse((root/'head.json').exists())
        before=immutable_digests(root)
        resumed=self.run_child(root,'resume')
        self.assertNotEqual(killed,resumed['pid'])
        self.assertEqual(resumed['reads'],list(range(confirmed+1,4)))
        self.assertEqual(resumed['rows'],15);self.assertEqual(resumed['cursor'],3)
        self.assert_complete_numeric(root)
        after=immutable_digests(root)
        self.assertTrue(before.items()<=after.items(),'Existing immutable bytes changed')
        self.assertFalse(list(root.glob('*.pending-*')))
        head=(root/'head.json').read_bytes()
        repeated=self.run_child(root,'resume')
        self.assertNotEqual(repeated['pid'],resumed['pid'])
        self.assertEqual(repeated['reads'],[])
        self.assertEqual(immutable_digests(root),after)
        self.assertEqual((root/'head.json').read_bytes(),head)

    def test_sigkill_new_process_resumes_only_uncommitted_source_parts(self):
        # Original scenario retained, now through the same per-part entry as main.
        self.prove('after_checkpoint',2,2)

    def test_sigkill_partial_commit_boundaries(self):
        cases=[('after_numeric',2,1),('after_commit',2,1),('before_head',2,1),
               ('after_head',2,2),('after_commit',1,0)]
        for stage,ordinal,confirmed in cases:
            with self.subTest(stage=stage,ordinal=ordinal):
                self.prove(stage,ordinal,confirmed)

    def test_gzip_and_commit_bytes_identical_between_new_processes(self):
        left=self.root/'left';right=self.root/'right'
        left.mkdir(mode=0o700);right.mkdir(mode=0o700)
        first=self.run_child(left,'init');second=self.run_child(right,'init')
        self.assertNotEqual(first['pid'],second['pid'])
        self.assertEqual(first['reads'],[1,2,3]);self.assertEqual(second['reads'],[1,2,3])
        self.assertEqual(immutable_digests(left),immutable_digests(right))
        self.assertEqual((left/'head.json').read_bytes(),(right/'head.json').read_bytes())
        self.assert_complete_numeric(left);self.assert_complete_numeric(right)


if __name__=='__main__':
    if len(sys.argv)>1 and sys.argv[1]=='--profile16-worker':
        worker(sys.argv[2],sys.argv[3],sys.argv[4],int(sys.argv[5]))
    else:unittest.main()
