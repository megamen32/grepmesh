import hashlib
from types import SimpleNamespace
import unittest
import multi_cloud

class LiveRoutingTests(unittest.TestCase):
    path='/Users/user/Documents/LastHumanCommit/README.md'
    def readers(self):
        return [SimpleNamespace(scope=f'live-mac-m1-d038c011eeb7fe446596e71c-b{i:02}',
                partition_count=4,manifest={'source_host':'mac-m1','source_root':'/'}) for i in range(4)]
    def route(self, readers):
        return multi_cloud.route_live_readers(readers,self.path)
    def test_complete_live_group_routes_one_bucket(self):
        readers=self.readers()
        index=int.from_bytes(hashlib.sha256(self.path.encode()).digest()[:8],'big')%4
        self.assertEqual(self.route(readers),[readers[index]])
    def test_incomplete_or_unannotated_group_never_narrows(self):
        readers=self.readers()[:3]
        self.assertEqual(self.route(readers),readers)
        readers=self.readers();readers[0].partition_count=None
        self.assertEqual(self.route(readers),readers)
    def test_mixed_count_host_root_and_duplicate_never_narrow(self):
        for field,value in [('count',3),('host','other'),('root','/Users')]:
            readers=self.readers()
            if field=='count':readers[0].partition_count=value
            else:readers[0].manifest['source_'+field]=value
            self.assertEqual(self.route(readers),readers)
        readers=self.readers();readers.append(readers[0])
        self.assertEqual(self.route(readers),readers)

if __name__=='__main__':unittest.main()
