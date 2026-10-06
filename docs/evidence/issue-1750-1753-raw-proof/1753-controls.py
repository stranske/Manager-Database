import pathlib,subprocess,tempfile,hashlib,json
p=pathlib.Path(__file__).parent;w=pathlib.Path('/Users/teacher/.codex/automations/imi-merge-verify-closer/worktrees/manager-1753-acceptance-1622');s=w/'api/managers.py';original=s.read_bytes();raw=original.decode();out=[]
start=raw.index('async def patch_manager(');prefix=raw[:start];tail=raw[start:]
mutations={'early-close':prefix+tail.replace('conn = connect_db()\n','conn = connect_db()\n        conn.close()\n').replace('            conn.close()\n','            pass\n'),'drop-retained-tags':raw.replace('merged_tags = _merge_tags(existing_tags, add_tags, remove_tags)','merged_tags = add_tags')}
try:
 for name,text in mutations.items():
  assert text!=raw;s.write_text(text)
  with tempfile.TemporaryDirectory(prefix='1753-cache-') as cache:
   r=subprocess.run(['/opt/anaconda3/bin/python3','-X','pycache_prefix='+cache,'-m','pytest','tests/test_manager_write_failures.py','-q','-o','addopts=','--junitxml='+str(p/f'1753-{name}-RED.xml')],cwd=w,text=True,capture_output=True)
  (p/f'1753-{name}-RED.txt').write_text(r.stdout+r.stderr);out.append({'mutation':name,'returncode':r.returncode});assert r.returncode==1
finally:s.write_bytes(original)
assert s.read_bytes()==original
r=subprocess.run(['/opt/anaconda3/bin/python3','-m','pytest','tests/test_manager_write_failures.py','-q','-o','addopts=','--junitxml='+str(p/'1753-restored-GREEN.xml')],cwd=w,text=True,capture_output=True);(p/'1753-restored-GREEN.txt').write_text(r.stdout+r.stderr);out.append({'restored':True,'sha256':hashlib.sha256(s.read_bytes()).hexdigest(),'returncode':r.returncode});(p/'1753-controls.json').write_text(json.dumps(out,indent=2));print(json.dumps(out));assert r.returncode==0
