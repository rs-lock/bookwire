#!/usr/bin/env python3
"""Untimed structural replay of ladder bitmap work; no latency claims.
Reproduce PriceLadder set/find_prev/find_next and compare its complete final
state with the measured reference. Count bitmap words read on best deletion,
occupancy transitions and distinct quantity pages written. Stdlib only.
"""
import argparse
import csv
import gzip
import json
from pathlib import Path
from json_to_sbe import parse_line, to_mantissa


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--input',type=Path,default=Path('crates/binance/replay/deltas.jsonl'))
    ap.add_argument('--out',type=Path,default=Path('benchmarks/orderbook'))
    a=ap.parse_args()
    capacity=700001
    quantities=[[0]*capacity for _ in range(2)]
    words=[[0]*((capacity+63)//64) for _ in range(2)]
    best=[None,None]
    seen_pages=set()
    rows=[]
    with a.input.open() as f:
        for line in f:
            d=parse_line(line)
            if d is None: continue
            counts=dict(message=len(rows)+1,updates=len(d['b'])+len(d['a']),bitmap_words_read=0,best_deletes=0,inserts=0,deletes=0,new_quantity_pages=0)
            for side,key in enumerate(('b','a')):
                for p,s in d[key]:
                    p=to_mantissa(p); size=to_mantissa(s)
                    if p<50000*10**8 or p>120000*10**8: continue
                    assert (p-50000*10**8)%10000000==0
                    i=(p-50000*10**8)//10000000
                    page=(side,i//512)
                    if page not in seen_pages:
                        seen_pages.add(page);counts['new_quantity_pages']+=1
                    old=quantities[side][i]
                    counts['inserts']+=int(old==0 and size!=0)
                    counts['deletes']+=int(old!=0 and size==0)
                    quantities[side][i]=size
                    if size: words[side][i//64]|=1<<(i%64)
                    else: words[side][i//64]&=~(1<<(i%64))
                    if size:
                        if best[side] is None or (side==0 and i>best[side]) or (side==1 and i<best[side]):best[side]=i
                    elif best[side]==i:
                        counts['best_deletes']+=1
                        pos=i-1 if side==0 else i+1
                        best[side]=None
                        if not 0<=pos<capacity:continue
                        w=pos//64
                        mask=(1<<((pos%64)+1))-1 if side==0 else ((1<<64)-1)<<(pos%64)
                        value=words[side][w]&mask
                        while True:
                            counts['bitmap_words_read']+=1
                            if value:
                                bit=value.bit_length()-1 if side==0 else (value&-value).bit_length()-1
                                best[side]=w*64+bit
                                break
                            w+=-1 if side==0 else 1
                            if not 0<=w<len(words[side]):break
                            value=words[side][w]
            rows.append(counts)
    # Same canonical SHA-256 encoding as OrderBook::state_hash.
    import hashlib,struct
    h=hashlib.sha256()
    for side,tag in enumerate((b'bids',b'asks')):
        h.update(tag)
        for i,q in enumerate(quantities[side]):
            if q:
                h.update(struct.pack('<q',50000*10**8+i*10000000));h.update(b':')
                h.update(struct.pack('<Q',q));h.update(b';')
    reference=json.loads((a.out/'correctness.json').read_text())
    assert h.hexdigest()==reference['book']['state_sha256']
    assert len(rows)==reference['messages']
    with gzip.open(a.out/'structural-diagnostics.csv.gz','wt') as f:
        writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
    result={'note':'Untimed model of exact bitmap traversal; quantity page numbers are logical offsets assuming 4 KiB pages, not OS residency measurements.', 'state_sha256':h.hexdigest(),'totals':{k:sum(r[k] for r in rows) for k in list(rows[0])[2:]},'largest_bitmap_scans':sorted(rows,key=lambda r:r['bitmap_words_read'],reverse=True)[:10]}
    (a.out/'structural-diagnostics.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))

if __name__=='__main__':main()
