import sys, json, importlib.util, numpy as np
from pathlib import Path
HERE=Path(__file__).resolve().parent
sys.path[:0]=[str(HERE.parent / "src")]
spec=importlib.util.spec_from_file_location("lr", str(HERE / "live_race.py")); lr=importlib.util.module_from_spec(spec); spec.loader.exec_module(lr)
from aas_poll_research.interface import ResearchDiscoveryObservation, PollContext
d=json.load(open(HERE / "results" / "race1.json"))
first=next(iter(d["pollers"].values()))
ready=np.array(sorted(r for r,_ in first["found"].values())); ready-=ready[0]          # real arrival pattern (A clock, relative)
qs=[s["te"]-s["ts"] for p in d["pollers"].values() for s in p["scans"] if s["ok"]]; Q=float(np.median(qs)); print("measured scan duration median %.2fs, real arrivals %d"%(Q,len(ready)))
base=1.8e9; rng=np.random.default_rng(1); R=60
res={n:{"D":[],"N":[]} for n in lr.CONTENDERS}
for rep in range(R):
    shift=rng.uniform(0,30); arr=ready+shift+2.0
    for n,f in lr.CONTENDERS.items():
        p=f(); t=0.0; seen=0; D=[]; N=0
        while seen<len(arr) and t<900:
            tsnap=t+Q*0.5; new=[a for a in arr if a<=tsnap][seen:]
            D+= [tsnap-a for a in new]; seen+=len(new); N+=1
            p.observe(ResearchDiscoveryObservation(len(new),len(new),1,t+Q,base+tsnap,tuple(base+a for a in new)))
            t+=Q+max(0.3,p.next_delay_seconds(PollContext(t+Q,False,False)))
        res[n]["D"].append(np.mean(D)); res[n]["N"].append(N)
print(f"\n{R} random start phases; mean(+-sd across phases)")
print(f"{'policy':26s} {'scans':>8s} {'meanD s':>14s} {'J(c=3)':>9s} {'J(c=10)':>9s} {'J(c=30)':>9s}")
rows=[]
for n,v in res.items():
    D=np.array(v["D"]); N=np.array(v["N"]); tot=D*len(ready)
    J={c:(c*N+tot)/60 for c in (3,10,30)}
    rows.append((n,N.mean(),D.mean(),D.std(),{c:(J[c].mean(),J[c].std()) for c in J}))
for r in sorted(rows,key=lambda r:r[4][10][0]):
    print(f"{r[0]:26s} {r[1]:8.1f} {r[2]:7.2f}+-{r[3]:4.2f} "+" ".join(f"{r[4][c][0]:6.1f}({r[4][c][1]:.1f})" for c in (3,10,30)))
for c in (3,10,30):
    b=min(rows,key=lambda r:r[4][c][0]); print(f"best J(c={c}): {b[0]}")
