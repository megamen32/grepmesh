"""Pure numeric packing model. No filesystem, processes or transport imports.

The runtime adapter supplies the existing reviewed Limits instance explicitly.
This module does not create a second set of runtime limits or a new policy.
"""
from __future__ import annotations
import copy

MIB = 1024 * 1024
MAX_DOCUMENT = MIB

def empty_bucket():
    return dict(rows=0,searchable=0,oversized=0,body_bytes=0,searchable_body_bytes=0,
        max_body_bytes=0,max_line_bytes=0,max_path_bytes=0,searchable_path_bytes=0,searchable_fold_bytes=0,
        path_primary_table_payload_lower_bound=0,packed_frames_bytes=0,independent_body_zlib_bytes=0,
        trigram_occurrences_upper_bound=0,packing_groups=0,full_groups=0,byte_flushes=0,
        pressure_flushes=0,batch_tails=0,group_min_documents=0,group_max_documents=0,
        group_max_body_bytes=0,group_max_frame_bytes=0,minimum_sqlite_scratch_available_bytes=64*MIB)

class Model:
    """Exact deterministic fill-policy counters; no paths/text retained."""
    def __init__(self,state=None,*,limits):
        self.limits=limits;self.limits.validate()
        self.cap=min(16*MIB,self.limits.max_sqlite_bytes//4)
        self.state=copy.deepcopy(state) if state is not None else {
            'rows':0,'last_rowid':0,'body_bytes':0,'partitions':[empty_bucket() for _ in range(16)],
            'queue':[dict(n=0,body=0,frames=0) for _ in range(16)],'peak_queue_bytes':0,
            'peak_queue_documents':0,'max_decoded_group_bytes':0,'max_decoded_group_documents':0}
        self._validate()
    def _validate(self):
        s=self.state
        if len(s['partitions'])!=16 or len(s['queue'])!=16:raise ValueError('Bad profile dimensions')
        if any(type(v) is not int or v<0 for q in s['queue'] for v in q.values()):raise ValueError('Bad queue counters')
        if any(q['n']>self.limits.pending_documents or q['body']>self.limits.pending_body_bytes for q in s['queue']):raise ValueError('Queue exceeds original admission')
        if sum(q['frames'] for q in s['queue'])>self.cap:raise ValueError('Total queue exceeds original admission')
        if sum(b['rows'] for b in s['partitions'])!=s['rows']:raise ValueError('Profile row count mismatch')
    def flush(self,pid,reason):
        s=self.state;q=s['queue'][pid]
        if not q['n']:return
        b=s['partitions'][pid];remaining=sum(x['frames'] for x in s['queue'])-q['frames']
        scratch=((self.limits.max_sqlite_bytes-remaining)//4096)*4096
        b['minimum_sqlite_scratch_available_bytes']=min(b['minimum_sqlite_scratch_available_bytes'],scratch)
        b['packing_groups']+=1
        b[{'full':'full_groups','bytes':'byte_flushes','pressure':'pressure_flushes','batch_tail':'batch_tails'}[reason]]+=1
        b['group_min_documents']=min(b['group_min_documents'] or q['n'],q['n'])
        b['group_max_documents']=max(b['group_max_documents'],q['n'])
        b['group_max_body_bytes']=max(b['group_max_body_bytes'],q['body'])
        b['group_max_frame_bytes']=max(b['group_max_frame_bytes'],q['frames'])
        s['max_decoded_group_bytes']=max(s['max_decoded_group_bytes'],q['body'])
        s['max_decoded_group_documents']=max(s['max_decoded_group_documents'],q['n'])
        s['queue'][pid]=dict(n=0,body=0,frames=0)
    def add(self,pid,rowid,body_size,path_size,fold_size,line_size,frame_size,compressed_size,trigram_upper):
        s=self.state
        if not 0<=pid<16 or rowid<=s['last_rowid']:raise ValueError('Profile source order/partition mismatch')
        if any(type(x) is not int or x<0 for x in (body_size,path_size,fold_size,line_size,frame_size,compressed_size,trigram_upper)):raise ValueError('Invalid numeric metadata')
        b=s['partitions'][pid];b['rows']+=1;b['body_bytes']+=body_size
        b['max_body_bytes']=max(b['max_body_bytes'],body_size);b['max_path_bytes']=max(b['max_path_bytes'],path_size);b['max_line_bytes']=max(b['max_line_bytes'],line_size)
        s['rows']+=1;s['body_bytes']+=body_size;s['last_rowid']=rowid
        if body_size>MAX_DOCUMENT:b['oversized']+=1;return
        b['searchable']+=1;b['searchable_body_bytes']+=body_size
        b['searchable_path_bytes']+=path_size;b['searchable_fold_bytes']+=fold_size
        # Primary-table string payload ONLY. No unproved B-tree multiplier.
        b['path_primary_table_payload_lower_bound']+=path_size+fold_size+64
        b['packed_frames_bytes']+=frame_size;b['independent_body_zlib_bytes']+=compressed_size
        b['trigram_occurrences_upper_bound']+=trigram_upper
        q=s['queue'][pid]
        if q['n']>=self.limits.pending_documents or q['body']+body_size>self.limits.pending_body_bytes:self.flush(pid,'bytes')
        if frame_size>self.cap:raise ValueError('Frame exceeds original quarter-scratch admission')
        while sum(x['frames'] for x in s['queue'])+frame_size>self.cap:
            largest=max(range(16),key=lambda i:s['queue'][i]['frames'])
            if not s['queue'][largest]['n']:raise ValueError('Packing cannot make progress')
            self.flush(largest,'pressure')
        q=s['queue'][pid];q['n']+=1;q['body']+=body_size;q['frames']+=frame_size
        s['peak_queue_bytes']=max(s['peak_queue_bytes'],sum(x['frames'] for x in s['queue']))
        s['peak_queue_documents']=max(s['peak_queue_documents'],sum(x['n'] for x in s['queue']))
        if q['n']==self.limits.pending_documents:self.flush(pid,'full')
    def batch_end(self):
        for i in range(16):self.flush(i,'batch_tail')
    def summary(self,*,complete=False):
        out=[]
        for pid,b in enumerate(self.state['partitions']):
            out.append({'partition':pid,**copy.deepcopy(b),
                'packing_group_lower_bound_on_successful_segments':b['packing_groups'],
                'sqlite_segments_upper_bound_if_build_succeeds':b['searchable'],
                'additional_sqlite_splits_upper_bound_if_build_succeeds':b['searchable']-b['packing_groups'],
                'sqlite_upper_bound_basis':'binary split cannot produce more successful leaves than input documents; singleton fit NOT proved',
                'sqlite_raw_bytes_upper_bound':'enforced 64MiB per file, not proof of successful admission',
                'path_index_raw_bytes_upper_bound':None,
                'path_index_payload_exceeds_cap':b['path_primary_table_payload_lower_bound']>self.limits.max_sqlite_bytes,
                'packing_count_exceeds_500':b['packing_groups']>self.limits.max_shards_per_partition})
        return {'complete_source_profile':complete,'rows':self.state['rows'],
            'searchable':sum(b['searchable'] for b in out),'oversized':sum(b['oversized'] for b in out),
            'body_bytes':self.state['body_bytes'],'partitions':out,'peak_queue_bytes':self.state['peak_queue_bytes'],
            'max_decoded_group_bytes':self.state['max_decoded_group_bytes'],
            'max_decoded_group_documents':self.state['max_decoded_group_documents'],
            'capacity_rejection_proved':any(b['packing_count_exceeds_500'] or b['path_index_payload_exceeds_cap'] for b in out),
            'full_snapshot_capacity_accepted':False,
            'sqlite_and_path_index_size_admission':'not measured; numeric profile is not an FTS build',
            'term_metric':'raw character-window count before SQLite normalization, not a storage bound',
            'actual_cloud_FTS_segments_created':0}
