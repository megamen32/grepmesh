#!/usr/bin/python3
"""Source-only regression; no real signer/client/cloud request or acceptance."""
import copy
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('ranking_bridge',
    Path(__file__).with_name('hub_signed_mcp_bridge.py'))
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)


def hit(path, text, line=1):
    return {'path': path, 'text': text, 'source_host': 'server-44',
            'host_id': 'cloud-text', 'line_number': line, 'context': [],
            'body_sha256': 'a' * 64, 'snapshot_at': 1234}


def payload(hits, **extra):
    return {'results': hits, 'partial': False, 'truncated': False,
            'warnings': [], **extra}


def policy():
    return {'io.gptadmin/filesystem-acl': {'version': 1,
        'profile_id': bridge.PROFILE_ID, 'profile_version': 1,
        'target': bridge.EXPECTED_TARGET, 'tool': 'mcp_call',
        'job_id': 'unit-only-not-e2e', 'roots_digest': 'a' * 64,
        'args_sha256': 'b' * 64, 'read_roots': ['/home']}}


class CloudRankContractTest(unittest.TestCase):
    def test_default_and_boolean_optout(self):
        self.assertEqual(bridge._search_plan({'query': 'x'}), (True, 20))
        self.assertEqual(bridge._search_plan({'query': 'x', 'rank': False}), (False, 20))
        for value in (None, 0, 1, 'false', [], {}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                bridge._search_plan({'query': 'x', 'rank': value})

    def test_invalid_query_and_limit_fail_closed(self):
        for args in ({'query': ''}, {'query': []}, {'query': 'x' * 2049},
                     {'query': 'x', 'limit': True}, {'query': 'x', 'limit': '20'}):
            with self.subTest(args=args), self.assertRaises(ValueError):
                bridge._search_plan(args)

    def test_scoring_precedes_top_k(self):
        p = payload([hit('/home/a.txt', 'needle reference'),
                     hit('/home/z/needle.py', 'def needle():')])
        out = bridge._rank_authorized_payload(p, {'query': 'needle'}, True, 1)
        self.assertEqual(out['results'][0]['path'], '/home/z/needle.py')
        self.assertEqual(out['ranking']['candidate_count'], 2)
        self.assertTrue(out['truncated'])
        self.assertTrue(out['ranking']['before_top_k'])

    def test_optout_preserves_backend_order(self):
        hits = [hit('/home/z.txt', 'needle'), hit('/home/a.txt', 'def needle():')]
        hits[0]['score'] = 100
        out = bridge._rank_authorized_payload(payload(hits), {'query': 'needle'}, False, 2)
        self.assertEqual([x['path'] for x in out['results']], [x['path'] for x in hits])
        self.assertFalse(any('score' in x for x in out['results']))

    def test_no_mutation_of_raw_results(self):
        p = payload([hit('/home/a', 'needle')]); before = copy.deepcopy(p)
        bridge._rank_authorized_payload(p, {'query': 'needle'}, True, 1)
        self.assertEqual(p, before)

    def test_regex_short_unicode_and_missing_history_never_filtered(self):
        hits = [hit('/home/deleted.txt', 'OCR: Привет мир'),
                hit('/home/audio.txt', 'STT spoken words'), hit('/home/a.txt', 'x')]
        for query in ('x', 'Привет', '(?:a|b){2}', '.*', 'audio'):
            with self.subTest(query=query):
                a = bridge._rank_authorized_payload(payload(hits), {'query': query}, True, 200)
                b = bridge._rank_authorized_payload(payload(hits), {'query': query}, False, 200)
                self.assertEqual({h['path'] for h in a['results']}, {h['path'] for h in b['results']})
                self.assertEqual(len(a['results']), 3)

    def test_ranking_has_no_filesystem_or_network(self):
        p = payload([hit('/home/nonexistent/deleted', 'needle')])
        with patch('builtins.open', side_effect=AssertionError('file IO')):
            with patch.object(bridge.urllib.request, 'urlopen', side_effect=AssertionError('network')):
                out = bridge._rank_authorized_payload(p, {'query': 'needle'}, True, 20)
        self.assertEqual(len(out['results']), 1)

    def test_ties_are_deterministic(self):
        p = payload([hit('/home/z', 'needle', 2), hit('/home/a', 'needle', 3), hit('/home/a', 'needle', 1)])
        out = bridge._rank_authorized_payload(p, {'query': 'needle'}, True, 3)
        self.assertEqual([(h['path'], h['line_number']) for h in out['results']],
                         [('/home/a', 1), ('/home/a', 3), ('/home/z', 2)])

    def test_partial_and_truncated_coverage_not_upgraded(self):
        p = payload([hit('/home/a', 'needle')], partial=True, truncated=True,
                    warnings=['remaining scopes not searched'])
        out = bridge._rank_authorized_payload(p, {'query': 'needle'}, True, 200)
        self.assertTrue(out['partial']); self.assertTrue(out['truncated'])
        self.assertEqual(out['warnings'], p['warnings'])
        self.assertFalse(out['ranking']['candidate_scan_complete'])

    def test_candidate_limit_is_reported_and_bounded(self):
        p = payload([hit('/home/' + str(i), 'needle') for i in range(200)])
        out = bridge._rank_authorized_payload(p, {'query': 'needle'}, True, 20)
        self.assertEqual(out['ranking']['candidate_limit'], 200)
        self.assertFalse(out['ranking']['candidate_scan_complete'])
        p['results'].append(hit('/home/extra', 'needle'))
        with self.assertRaises(RuntimeError):
            bridge._rank_authorized_payload(p, {'query': 'needle'}, True, 20)

    def test_malformed_results_refused(self):
        for results in (None, {}, [None], [{'path': 10}], [hit('/home/a', 'x', -1)]):
            with self.subTest(results=results), self.assertRaises(RuntimeError):
                bridge._rank_authorized_payload(payload(results), {'query': 'x'}, True, 20)

    def test_both_modes_request_same_candidates_once(self):
        response = {'isError': False, 'content': [{'type': 'text', 'text': json.dumps(payload([
            hit('/home/a', 'needle'), hit('/home/z/needle.py', 'def needle():')]))}]}
        forwarded = []
        for rank in (True, False):
            args = {'query': 'needle', 'hosts': 'server-44', 'limit': 1, 'rank': rank}
            before = copy.deepcopy(args)
            with patch.object(bridge, '_remote_signed_call', return_value=response) as remote:
                result = bridge.call('search', args, policy())
            remote.assert_called_once(); forwarded.append(remote.call_args.args[1])
            self.assertEqual(args, before)
            p = json.loads(result['content'][0]['text'])
            self.assertTrue(p['partial']); self.assertEqual(len(p['results']), 1)
            self.assertEqual(p['ranking']['enabled'], rank)
        for args in forwarded:
            self.assertEqual(args['limit'], 200); self.assertEqual(args['max_matches'], 200)

    def test_upstream_acl_denial_not_transformed(self):
        denied = {'isError': True, 'content': [{'type': 'text', 'text': 'ACL denied'}]}
        with patch.object(bridge, '_remote_signed_call', return_value=denied):
            result = bridge.call('search', {'query': 'x', 'hosts': 'server-44'}, policy())
        self.assertEqual(result, denied)

    def test_bad_rank_rejected_before_signed_call(self):
        with patch.object(bridge, '_remote_signed_call', side_effect=AssertionError('network')):
            with self.assertRaises(ValueError):
                bridge.call('search', {'query': 'x', 'hosts': 'server-44', 'rank': 'false'}, policy())

    def test_read_text_does_not_request_search_candidates(self):
        args = {'host': 'server-44', 'path': '/home/example'}
        response = {'isError': False, 'content': [{'type': 'text', 'text': json.dumps({'path': '/home/example', 'chunks': []})}]}
        with patch.object(bridge, '_remote_signed_call', return_value=response) as remote:
            bridge.call('read_text', args, policy())
        self.assertEqual(remote.call_args.args[1], args)


if __name__ == '__main__':
    unittest.main()
