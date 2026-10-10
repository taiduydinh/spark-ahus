import pathlib,sys,json
sys.path.insert(0,str(pathlib.Path(__file__).parent))
import run_ahus_paper_protocol as r
p=r.load()
assert len(p['groups'])==14
assert sum(len(x['cases']) for x in p['groups'].values())==61
assert p['groups']['bible_fixed_L3']['cases'][0]['max_pattern_length']==3
assert p['groups']['sign_pruning']['cases'][0]['max_pattern_length'] is None
assert p['groups']['bms2_unrestricted']['cases'][0]['max_pattern_length']==0
assert p['groups']['kosarak_scalability']['cases'][-1]['first_n_sequences']==600000
print('PASS: protocol schema and key length/threshold properties')
