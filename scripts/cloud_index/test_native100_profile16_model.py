"""Source-only candidate: pure model cases, no file setUp or transport import.

This organization is not authorization to execute a subset of a denied action.
No tests in this patch have been run.
"""
from dataclasses import dataclass
import json
import unittest
from native100_profile16_model import Model


@dataclass(frozen=True)
class ExampleLimits:
    """Synthetic configuration values only; not a new production policy."""
    pending_body_bytes: int = 8 * 1024 * 1024
    pending_documents: int = 512
    max_sqlite_bytes: int = 64 * 1024 * 1024
    max_shards_per_partition: int = 500

    def validate(self):
        if vars(self) != vars(ExampleLimits()):
            raise ValueError('This fixture covers the reviewed default values only')


class PureModelTests(unittest.TestCase):
    # No setUp, TemporaryDirectory, chmod, cleanup or native imports.
    def test_model_survives_part_boundary_without_draining_queues(self):
        m=Model(limits=ExampleLimits())
        for n in range(1,301):m.add(0,n,100,20,20,200,80,60,100)
        restored=Model(json.loads(json.dumps(m.state)),limits=ExampleLimits())
        for n in range(301,514):restored.add(0,n,100,20,20,200,80,60,100)
        self.assertEqual(restored.state['partitions'][0]['full_groups'],1);self.assertEqual(restored.state['queue'][0]['n'],1)
        restored.batch_end();self.assertEqual(restored.state['partitions'][0]['batch_tails'],1)
    def test_success_upper_bound_not_confused_with_capacity_acceptance(self):
        m=Model(limits=ExampleLimits())
        for n in range(1,601):m.add(0,n,1,20,20,200,60,9,20)
        m.batch_end();s=m.summary(complete=True);b=s['partitions'][0]
        self.assertEqual(b['packing_group_lower_bound_on_successful_segments'],2)
        self.assertEqual(b['sqlite_segments_upper_bound_if_build_succeeds'],600)
        self.assertFalse(s['full_snapshot_capacity_accepted']);self.assertIsNone(b['path_index_raw_bytes_upper_bound'])

if __name__=='__main__':unittest.main()
