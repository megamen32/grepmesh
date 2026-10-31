#!/usr/bin/env python3
"""Bounded synthetic direct-tree scan; no production source or remote cloud."""
import io
import json
import os
import pathlib
import subprocess
import tempfile

import scan_tree
from cloud_index import MAX_DOCUMENT, MemoryStore, Publisher, Reader, ensure_tmpfs

TMP = pathlib.Path('/run/user') / str(os.getuid()) / 'grepmesh-cloud-tree-tests'
ensure_tmpfs(TMP)


def run():
    checks = []
    def ok(name, assertion):
        assert assertion, name
        checks.append(name)
    with tempfile.TemporaryDirectory(dir=TMP) as work:
        root = pathlib.Path(work)
        prev = scan_tree.ALLOWED_PARENT
        scan_tree.ALLOWED_PARENT = root
        try:
            project = root / 'example-project'
            project.mkdir()
            (project / 'main.ts').write_text('export const uniqueNeedle = 19;\n',encoding='utf-8')
            (project / 'README.md').write_text('Текст про поиск и обновление',encoding='utf-8')
            (project / 'view.vue').write_text('<template>Vue component</template>',encoding='utf-8')
            (project / 'layout.templ').write_text('templ Hello() { Hello! }',encoding='utf-8')
            (project / 'auth.json').write_text('SECRET_NEVER_EXPORTED',encoding='utf-8')
            (project / '.env').write_text('SECRET_ENV_NEVER_EXPORTED',encoding='utf-8')
            (project / 'binary.py').write_bytes(b'\0binary')
            (project / 'huge.ts').write_bytes(b'x' * (MAX_DOCUMENT + 1))
            (project / 'escape.md').symlink_to('/etc/hosts')
            (project / 'node_modules').mkdir()
            (project / 'node_modules' / 'irrelevant.js').write_text('INVISIBLE_DEPENDENCY')
            for ignored in ('.tmp', '.worktrees', 'var', 'graphify-out', 'dist.backup-20261031', '.tmpbin'):
                (project / ignored).mkdir()
                (project / ignored / 'garbage.ts').write_text('GENERATED_SHOULD_NOT_SEARCH')

            (project / 'nested').mkdir()
            (project / 'nested' / 'query.sql').write_text('SELECT count(*) FROM db;')
            (project / 'bad-link').symlink_to('/etc')
            stream = io.StringIO()
            scan_tree.scan_tree(project, 'server-100', stream)
            docs = [json.loads(x) for x in stream.getvalue().splitlines()]
            source = [x for x in docs if x.get('kind') == 'document']
            footer = docs[-1]
            ok('tracked and untracked text scanned',len(source)==5)
            ok('complete verified stream',footer['complete_for_policy'] and footer['count']==5)
            ok('Unicode text preserved',any('поиск' in x['body'] for x in source))
            ok('secret files rejected',footer['skipped'].get('secret_name')==2)
            ok('oversized files rejected',footer['skipped'].get('oversize')==1)
            ok('binary files rejected',footer['skipped'].get('binary')==1)
            ok('symlink files rejected',footer['skipped'].get('symlink_file')==1)
            ok('dependency directory ignored',footer['skipped'].get('generated_or_dependency_dir')==7)
            ok('symlink directory ignored',footer['skipped'].get('symlink_dir')==1)
            ok('no secret text exported',not any('SECRET' in x['body'] for x in source))
            store = MemoryStore()
            dest = root / 'ram'
            dest.mkdir()
            manifest = Publisher(store,dest).publish('project',iter(stream.getvalue().splitlines(keepends=True)))
            reader = Reader(store,dest,'project',64*1024*1024,'server-100',str(project))
            hits = reader.query('uniqueNeedle')
            ok('published text searchable',len(hits['results'])==1 and not hits['partial'])
            ok('Cyrillic searchable',len(reader.query('поиск')['results'])==1)
            ok('path lookup uses compact index',len(reader.query('*.vue',glob=True)['results'])==1)
            ok('no source-side SQLite',not list(project.rglob('*.sqlite')))
            ok('manifest row accounting',manifest['documents']==5)
            selected = io.StringIO()
            scan_tree.scan_tree(project,'server-100',selected,only_top=['nested'])
            chosen = [json.loads(line) for line in selected.getvalue().splitlines()]
            selected_docs = [item for item in chosen if item.get('kind')=='document']
            ok('top-level allowlist selects only chosen subtree',
               len(selected_docs)==1 and selected_docs[0]['path'].endswith('/nested/query.sql'))
            ok('top-level allowlist is recorded in the source policy',
               'allowed_top=nested' in chosen[0]['policy'])
            try:scan_tree.scan_tree(project,'server-100',io.StringIO(),only_top=['../escape'])
            except ValueError:ok('top-level selection refuses parent traversal',True)
            else:raise AssertionError('unsafe selector accepted')
            try:scan_tree.scan_tree(root,'server-100',io.StringIO())
            except ValueError:ok('broad root refused',True)
            else:raise AssertionError('Broad root scanned')
            try:scan_tree.scan_tree(project,'server-100',io.StringIO(),max_files=2)
            except RuntimeError:ok('document cap fails closed',True)
            else:raise AssertionError('Document cap ignored')
            try:scan_tree.scan_tree(project,'server-100',io.StringIO(),max_text_bytes=10)
            except RuntimeError:ok('byte cap fails closed',True)
            else:raise AssertionError('Byte cap ignored')
        finally:
            scan_tree.ALLOWED_PARENT = prev
    print(json.dumps({'passed':True,'tests':len(checks),'cases':checks},ensure_ascii=False))


if __name__=='__main__':
    if hasattr(os,'sched_setaffinity'):
        os.sched_setaffinity(0,sorted(os.sched_getaffinity(0))[:2])
    os.nice(10)
    run()
