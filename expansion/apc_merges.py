"""APC time-period corrections proposed for review; source workbook stays intact."""
import pandas as pd
import numpy as np
from expansion.engine import GROUP, text

def merge_apc(tables):
    original=tables['APC'].copy();updated=original.copy()
    counts=tables['OD'].copy()
    for c in GROUP:counts[c]=counts[c].map(text)
    count=counts.groupby(GROUP).size().to_dict()
    modes=['ON','OFF'];periods=list(range(7));log=[]
    valid=original[original.ROUTE_SURVEYED.notna() & (original.ROUTE_SURVEYED.map(text)!='') & (original.Segment.map(text)!='-')].copy()
    for (route,direction),group in valid.groupby(['ROUTE_SURVEYED','FINAL_ROUTE_DIRECTION'],sort=False):
        route=text(route);direction=text(direction);indices=list(group.index)
        def complete(p):return all(pd.to_numeric(group[f'{p}_{mode}'],errors='raise').notna().all() for mode in modes)
        surveyed=[p for p in periods if count.get((route,direction,str(p)),0)>0]
        targets=[p for p in surveyed if complete(p)]
        for source in periods:
            if source in surveyed:continue
            source_on=pd.to_numeric(group[f'{source}_ON'],errors='raise');source_off=pd.to_numeric(group[f'{source}_OFF'],errors='raise')
            if not complete(source):
                log.append({'ROUTE_SURVEY':route,'FINAL_ROUTE_DIRECTION':direction,'FROM_TIME_PERIOD':source,'TO_TIME_PERIOD':None,'ON_MOVED':None,'OFF_MOVED':None,'STATUS':'Not merged: missing APC input'})
                continue
            on=float(source_on.sum());off=float(source_off.sum())
            if on<=1e-8 and off<=1e-8:continue
            if not targets:
                log.append({'ROUTE_SURVEY':route,'FINAL_ROUTE_DIRECTION':direction,'FROM_TIME_PERIOD':source,'TO_TIME_PERIOD':None,'ON_MOVED':0,'OFF_MOVED':0,'STATUS':'Not merged: no surveyed period with complete APC'})
                continue
            preferred={2:1,3:4}.get(source)
            if preferred in targets:
                target=preferred
            else:
                target=min(targets,key=lambda p:(abs(p-source),p))
            for mode in modes:
                sc=f'{source}_{mode}';tc=f'{target}_{mode}'
                # Each segment retains its own ON/OFF amount. Recipients already include prior additions.
                updated.loc[indices,tc]=pd.to_numeric(updated.loc[indices,tc])+pd.to_numeric(original.loc[indices,sc])
                updated.loc[indices,sc]=0
            log.append({'ROUTE_SURVEY':route,'FINAL_ROUTE_DIRECTION':direction,'FROM_TIME_PERIOD':source,'TO_TIME_PERIOD':target,'ON_MOVED':on,'OFF_MOVED':off,'STATUS':'Merged'})
    cols=['ROUTE_SURVEY','FINAL_ROUTE_DIRECTION','FROM_TIME_PERIOD','TO_TIME_PERIOD','ON_MOVED','OFF_MOVED','STATUS']
    return updated,pd.DataFrame(log,columns=cols)
