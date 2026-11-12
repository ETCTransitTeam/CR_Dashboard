"""Reviewable adjustments and Excel-equivalent checks; no Excel formula execution."""
import hashlib,json,re,math
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from pathlib import Path
import numpy as np
import pandas as pd
import openpyxl
from expansion.engine import KEY, GROUP, text

def key(row):return tuple(text(row[k]) for k in KEY)
def fingerprint(tables):
    h=hashlib.sha256()
    for n in ['O2O','OD','APC','SAMPLING']:
        cols=KEY if n in ['O2O','OD'] else ['ROUTE_SURVEYED','FINAL_ROUTE_DIRECTION','Segment']+[f'{i}_{m}' for i in range(7) for m in ['ON','OFF']] if n=='APC' else ['ROUTE_SURVEYED','FINAL_ROUTE_DIRECTION','SAMPLING_RATE']
        records=[[text(v) for v in row] for row in tables[n][cols].itertuples(index=False,name=None)]
        h.update(json.dumps([n,sorted(records)],separators=(',',':')).encode())
    return h.hexdigest()

def import_legacy(path,result,tables):
    w=openpyxl.load_workbook(path,read_only=True,data_only=False)
    if 'EXPORT' not in w.sheetnames:return None
    sheet=w['EXPORT'];rows={};base={key(r):r for _,r in result['cells'].iterrows()}
    for row in sheet:
        if tuple(text(c.value) for c in row[:5]) not in base:continue
        vals=[c.value for c in row]
        rows[row[0].row]={'key':[text(v) for v in vals[:5]],'ridership':float(vals[5]) if isinstance(vals[5],(int,float)) else 0.0,'weight':vals[7]}
    w.close()
    base={key(r):r for _,r in result['cells'].iterrows()}
    if set(tuple(r['key']) for r in rows.values())!=set(base):return None
    rules=[];unknown=[]
    def resolve(i,seen=None):
        seen=set() if seen is None else seen
        if i in seen or i not in rows:raise ValueError('Invalid/circular legacy weight reference at row '+str(i))
        seen.add(i);v=rows[i]['weight']
        if isinstance(v,(int,float)):return {'kind':'fixed_weight','value':float(v)}
        if v==f'=F{i}/G{i}':return {'kind':'cell'}
        m=re.fullmatch(r'=H(\d+)',str(v))
        if m:return resolve(int(m[1]),seen)
        m=re.fullmatch(r'=IFERROR\(SUM\(F(\d+):F(\d+)\)/SUM\(G(\d+):G(\d+)\),"-"\)',str(v))
        if m and m[1]==m[3] and m[2]==m[4]:
            members=[rows[j]['key'] for j in range(int(m[1]),int(m[2])+1) if j in rows]
            if len(members)!=int(m[2])-int(m[1])+1:raise ValueError('Legacy pool contains missing rows')
            if len({tuple(k[:3]) for k in members})!=1:raise ValueError('Legacy pool crosses route/direction/time period')
            return {'kind':'pool','members':members}
        raise ValueError('Unsupported legacy formula at H'+str(i)+': '+str(v))
    for i,r in rows.items():
        b=base[tuple(r['key'])];rule=resolve(i)
        if rule['kind']!='cell':rules.append({'key':r['key'],**rule})
        if abs(r['ridership']-b.TABLE6_RIDERSHIP)>1e-6:rules.append({'key':r['key'],'kind':'ridership_override','value':r['ridership']})
    return {'input_fingerprint':fingerprint(tables),'rules':rules,'export_reference':[{'key':r['key'],'value':r['ridership']} for r in rows.values()],'source':'Existing EXPORT worksheet','precision':'10 significant digits for compatibility with saved Power BI export'}

def automatic_pools(d):
    """Greedy nearest segment group, deterministic ties. Never crosses GROUP."""
    pools={};unresolved=[]
    for _,g in d.groupby(GROUP,sort=True):
        if g.RIDERSHIP_USED.isna().any():continue
        groups=[{i} for i in g.index]
        def stats(m):
            x=d.loc[sorted(m)];return float(x.RIDERSHIP_USED.sum()),float(x.SURVEYS.fillna(0).sum())
        def bad(m):
            f,n=stats(m);limit=d.loc[sorted(m),'MAX_WEIGHT_FACTOR'].min()
            # Empty cells with no target and no surveys need no pooling.
            if n==0:return f>1e-8
            weight=f/n
            return weight<=0.25 or (pd.notna(limit) and weight>limit)
        while len(groups)>1:
            candidates=[m for m in groups if bad(m)]
            if not candidates:break
            m=min(candidates,key=lambda z:min(z))
            others=[v for v in groups if v is not m]
            def distance(v):
                return (min(abs(int(d.at[i,'BOARDING_SEG'])-int(d.at[j,'BOARDING_SEG']))+abs(int(d.at[i,'ALIGHTING_SEG'])-int(d.at[j,'ALIGHTING_SEG'])) for i in m for j in v),min(v))
            other=min(others,key=distance);groups=[v for v in groups if v is not m and v is not other]+[m|other]
        for m in groups:
            if len(m)>1:
                for i in m:pools[i]=m
            if bad(m):unresolved.extend(m)
    return pools,unresolved

def finalize(result,tables,legacy=None,auto=True,manual_rules=None):
    d=result['cells'].copy();d['RAW_WEIGHT']=d.WEIGHT;d['RIDERSHIP_USED']=d.TABLE6_RIDERSHIP
    d['WEIGHT_SOURCE']='Cell';d['POOL']='';d['FINAL_WEIGHT']=d.WEIGHT
    log=[];mapping={key(r):i for i,r in d.iterrows()};used_legacy=legacy is not None and legacy.get('input_fingerprint')==fingerprint(tables)
    if used_legacy:
        # Preserve the exact pasted export precision only when every source input matches.
        for ref in legacy.get('export_reference',[]):d.at[mapping[tuple(ref['key'])],'RIDERSHIP_USED']=ref['value']
        for r in legacy['rules']:
            i=mapping[tuple(r['key'])]
            if r['kind']=='ridership_override':d.at[i,'RIDERSHIP_USED']=r['value'];log.append({'ACTION':'Existing ridership override',**dict(zip(KEY,r['key'])),'VALUE':r['value'],'MEMBERS':''})
        d['FINAL_WEIGHT']=d.RIDERSHIP_USED/d.SURVEYS
        for r in legacy['rules']:
            i=mapping[tuple(r['key'])]
            if r['kind']=='pool':
                members=[mapping[tuple(k)] for k in r['members']];den=d.loc[members,'SURVEYS'].fillna(0).sum()
                d.at[i,'FINAL_WEIGHT']=d.loc[members,'RIDERSHIP_USED'].sum()/den if den else np.nan
                d.at[i,'WEIGHT_SOURCE']='Existing pool';d.at[i,'POOL']='; '.join(k[3]+'>'+k[4] for k in r['members'])
            elif r['kind']=='fixed_weight':d.at[i,'FINAL_WEIGHT']=r['value'];d.at[i,'WEIGHT_SOURCE']='Existing fixed weight'
            if r['kind']!='ridership_override':log.append({'ACTION':d.at[i,'WEIGHT_SOURCE'],**dict(zip(KEY,r['key'])),'VALUE':d.at[i,'FINAL_WEIGHT'],'MEMBERS':d.at[i,'POOL']})
    elif auto:
        pools,unresolved=automatic_pools(d)
        for i,m in pools.items():
            members=sorted(m);den=d.loc[members,'SURVEYS'].fillna(0).sum()
            d.at[i,'FINAL_WEIGHT']=d.loc[members,'RIDERSHIP_USED'].sum()/den if den else np.nan
            d.at[i,'WEIGHT_SOURCE']='Automatic nearby pool';d.at[i,'POOL']='; '.join(d.at[j,'BOARDING_SEG']+'>'+d.at[j,'ALIGHTING_SEG'] for j in members)
            log.append({'ACTION':d.at[i,'WEIGHT_SOURCE'],**{k:d.at[i,k] for k in KEY},'VALUE':d.at[i,'FINAL_WEIGHT'],'MEMBERS':d.at[i,'POOL']})
    for rule in manual_rules or []:
        members=[mapping[tuple(k)] for k in rule['members']]
        if len({tuple(d.loc[i,GROUP]) for i in members})!=1:raise ValueError('A pool cannot cross route/direction/time period')
        den=d.loc[members,'SURVEYS'].fillna(0).sum()
        value=rule.get('weight',d.loc[members,'RIDERSHIP_USED'].sum()/den if den else np.nan)
        for i in members:
            d.at[i,'FINAL_WEIGHT']=value;d.at[i,'WEIGHT_SOURCE']='Reviewed pool' if 'weight' not in rule else 'Reviewed fixed weight'
            d.at[i,'POOL']='; '.join(d.at[j,'BOARDING_SEG']+'>'+d.at[j,'ALIGHTING_SEG'] for j in members)
            log.append({'ACTION':d.at[i,'WEIGHT_SOURCE'],**{k:d.at[i,k] for k in KEY},'VALUE':value,'MEMBERS':d.at[i,'POOL']})
    d['FINAL_EXPANDED']=d.SURVEYS.fillna(0)*d.FINAL_WEIGHT
    d.loc[d.SURVEYS.fillna(0)==0,'FINAL_EXPANDED']=0
    def quant(v,n,mode):
        def one(x):return float(Decimal(format(x,'.15g')).quantize(Decimal(1).scaleb(-n),rounding=mode)) if pd.notna(x) and np.isfinite(x) else np.nan
        return v.map(one) if isinstance(v,pd.Series) else one(v)
    def trunc(v,n):return quant(v,n,ROUND_DOWN)
    def excel_round(v,n):return quant(v,n,ROUND_HALF_UP)
    # Excel uses route + time period for these reconciliations, omitting the label.
    rg=['ROUTE_SURVEY','TIME_PERIOD']
    d['F_TOTAL']=d.groupby(rg).RIDERSHIP_USED.transform('sum').map(lambda v:trunc(v,6))
    d['K_TOTAL']=d.groupby(rg).FINAL_EXPANDED.transform('sum').map(lambda v:trunc(v,6))
    d.loc[d.APC_TOTAL.isna(),['F_TOTAL','K_TOTAL']]=np.nan
    d['NORMALIZED_RIDERSHIP']=d.FINAL_EXPANDED/d.K_TOTAL*d.APC_TOTAL
    d['FINAL_NORMALIZED_WEIGHT']=d.NORMALIZED_RIDERSHIP/d.SURVEYS
    d.loc[d.SURVEYS.fillna(0)==0,'FINAL_NORMALIZED_WEIGHT']=0
    od=tables['OD'].copy()
    for c in KEY:od[c]=od[c].map(text)
    countkeys=['ROUTE_SURVEY','TIME_PERIOD','BOARDING_SEG','ALIGHTING_SEG']
    direct=od.groupby(countkeys).size().to_dict()
    d['OD_COUNT']=[direct.get(tuple(r[c] for c in countkeys),0) for _,r in d.iterrows()]
    d['SURVEY_TOTAL']=d.groupby(rg).SURVEYS.transform('sum')
    direct_group=od.groupby(rg).size().to_dict()
    d['OD_TOTAL']=[direct_group.get(tuple(r[c] for c in rg),0) for _,r in d.iterrows()]
    d['FLAG_RIDERSHIP']=(excel_round(d.F_TOTAL,5)!=excel_round(d.APC_TOTAL,5)).astype(int)
    d['FLAG_EXPANDED']=(trunc(d.K_TOTAL,5)!=trunc(d.APC_TOTAL,5)).astype(int)
    # Floating point truncation noise below 1e-7 is separately tolerated in app diagnostics.
    d['FLAG_CELL_SURVEYS']=(d.SURVEYS.fillna(0)!=d.OD_COUNT).astype(int)
    d['FLAG_TIME_SURVEYS']=(d.SURVEY_TOTAL!=d.OD_TOTAL).astype(int)
    route_apc=tables['APC'].copy();route_apc['ROUTE']=route_apc.ROUTE_SURVEYED.map(text).str.rsplit('_',n=1).str[0]
    route_apc['TOTAL']=route_apc[[f'{i}_ON' for i in range(7)]].apply(pd.to_numeric).sum(axis=1)
    rt=route_apc.groupby('ROUTE').TOTAL.sum().to_dict();d['ROUTE_APC']=d.ROUTE.map(rt)
    d['ROUTE_EXPANDED']=d.groupby('ROUTE').FINAL_EXPANDED.transform('sum')
    incomplete_routes=set(d.loc[d.APC_TOTAL.isna(),'ROUTE'])
    d.loc[d.ROUTE.isin(incomplete_routes),['ROUTE_APC','ROUTE_EXPANDED']]=np.nan
    d['FLAG_ROUTE_RIDERSHIP']=(trunc(d.ROUTE_EXPANDED,4)!=trunc(d.ROUTE_APC,4)).astype(int)
    od['ROUTE']=od.ROUTE_SURVEY.str.rsplit('_',n=1).str[0];oc=od.groupby('ROUTE').size().to_dict()
    d['ROUTE_SURVEYS']=d.groupby('ROUTE').SURVEYS.transform('sum');d['ROUTE_OD']=d.ROUTE.map(oc)
    d['FLAG_ROUTE_SURVEYS']=(d.ROUTE_SURVEYS!=d.ROUTE_OD).astype(int)
    threshold=300/result['info']['excel_sampling_rate_percent']
    varied=d.groupby('ROUTE_SURVEY').FINAL_WEIGHT.transform('max')!=d.groupby('ROUTE_SURVEY').FINAL_WEIGHT.transform('min')
    d['HWFACTOR']=((d.FINAL_WEIGHT>threshold)&varied).astype(int)
    d['NO_RIDERSHIP']=((d.groupby(rg).FINAL_WEIGHT.transform('sum')<=0)&(d.SURVEY_TOTAL>0)&(d.F_TOTAL==0)).astype(int)
    issues=[]
    for gap in result.get('missing_apc',[]):issues.append({'CHECK':'Missing APC input',**{k:gap[k] for k in KEY},'ACTUAL':None,'EXPECTED':None,'DETAIL':'Fill APC '+gap['SOURCE_COLUMN']+'; this route/time has unavailable ridership'})
    tests=[('Excel M: ridership vs APC','FLAG_RIDERSHIP','F_TOTAL','APC_TOTAL'),('Excel Q: expanded vs APC','FLAG_EXPANDED','K_TOTAL','APC_TOTAL'),('Excel U: high weight','HWFACTOR','FINAL_WEIGHT',None),('Excel X: no ridership','NO_RIDERSHIP','F_TOTAL',None),('Excel Z: segment survey count','FLAG_CELL_SURVEYS','SURVEYS','OD_COUNT'),('Excel AC: time survey count','FLAG_TIME_SURVEYS','SURVEY_TOTAL','OD_TOTAL'),('Excel AG: route ridership','FLAG_ROUTE_RIDERSHIP','ROUTE_EXPANDED','ROUTE_APC'),('Excel AJ: route survey count','FLAG_ROUTE_SURVEYS','ROUTE_SURVEYS','ROUTE_OD')]
    for _,r in d.iterrows():
        for label,flag,actual,expected in tests:
            if r[flag]:issues.append({'CHECK':label,**{k:r[k] for k in KEY},'ACTUAL':r[actual],'EXPECTED':r[expected] if expected else threshold if flag=='HWFACTOR' else None,'DETAIL':'Existing Excel rule; rounding and truncation preserved'})
        if r.SURVEYS>0 and r.FINAL_WEIGHT<=0.25:issues.append({'CHECK':'Final weight at or below 0.25',**{k:r[k] for k in KEY},'ACTUAL':r.FINAL_WEIGHT,'EXPECTED':0.25,'DETAIL':'Pool nearby segments; pooled weight must be greater than 0.25'})
        if r.FINAL_WEIGHT>r.MAX_WEIGHT_FACTOR:issues.append({'CHECK':'Final weight exceeds route limit',**{k:r[k] for k in KEY},'ACTUAL':r.FINAL_WEIGHT,'EXPECTED':r.MAX_WEIGHT_FACTOR,'DETAIL':'Pool requires review'})
        if pd.isna(r.SAMPLING_RATE):issues.append({'CHECK':'Missing route sampling rate',**{k:r[k] for k in KEY},'ACTUAL':None,'EXPECTED':None,'DETAIL':'Automatic high-weight limit unavailable'})
        if r.SURVEYS>0 and (not np.isfinite(r.FINAL_WEIGHT) or not np.isfinite(r.FINAL_NORMALIZED_WEIGHT)):issues.append({'CHECK':'Weight unavailable for survey',**{k:r[k] for k in KEY},'ACTUAL':None,'EXPECTED':None,'DETAIL':'Requires review'})
    from expansion.apc_merges import merge_apc
    result['updated_apc'],result['apc_merge_log']=merge_apc(tables)
    result['areas_to_fix']=areas_to_fix(d,tables)
    result['info']['areas_to_fix']=len(result['areas_to_fix'])
    result['cells']=d
    result['checks']=pd.DataFrame(issues,columns=['CHECK']+KEY+['ACTUAL','EXPECTED','DETAIL'])
    result['adjustments']=pd.DataFrame(log,columns=['ACTION']+KEY+['VALUE','MEMBERS'])
    result['info'].update({'adjustment_mode':'Existing Excel adjustments' if used_legacy else 'Automatic nearby pooling' if auto else 'No pooling','adjustment_rows':len(log),'final_expanded_ridership':float(d.FINAL_EXPANDED.sum()),'issue_rows':len(issues),'legacy_loaded_but_inputs_changed':legacy is not None and not used_legacy})
    result['groups']=d.groupby(GROUP,as_index=False).agg(APC_TOTAL=('APC_TOTAL','first'),FITTED_RIDERSHIP=('TABLE6_RIDERSHIP',lambda x:x.sum(min_count=1)),ADJUSTED_RIDERSHIP=('RIDERSHIP_USED',lambda x:x.sum(min_count=1)),EXPANDED_RIDERSHIP=('FINAL_EXPANDED',lambda x:x.sum(min_count=1)),OD_SURVEYS=('SURVEYS','sum'))
    return result


def areas_to_fix(cells,tables):
    """Compare independent APC boarding totals with OD counts by route/direction/time."""
    apc=tables['APC'].copy()
    apc=apc[apc.ROUTE_SURVEYED.notna() & (apc.ROUTE_SURVEYED.map(text)!='') & (apc.Segment.map(text)!='-')].copy()
    apc=apc.rename(columns={'ROUTE_SURVEYED':'ROUTE_SURVEY'})
    for k in GROUP[:2]:apc[k]=apc[k].map(text)
    valuecols=[f'{i}_ON' for i in range(7)]
    for c in valuecols:apc[c]=pd.to_numeric(apc[c],errors='raise')
    long=apc.melt(id_vars=GROUP[:2],value_vars=valuecols,var_name='TIME_PERIOD',value_name='RIDERSHIP')
    long.TIME_PERIOD=long.TIME_PERIOD.str.split('_').str[0]
    ridership=long.groupby(GROUP,as_index=False).RIDERSHIP.agg(lambda x:np.nan if x.isna().any() else x.sum())
    od=tables['OD'].copy()
    od=od[od.ROUTE_SURVEY.notna() & (od.ROUTE_SURVEY.map(text)!='')].copy()
    for k in GROUP:od[k]=od[k].map(text)
    counts=od.groupby(GROUP).size().rename('SURVEYS').reset_index()
    overall=ridership.merge(counts,on=GROUP,how='outer',validate='one_to_one')
    overall['SURVEYS']=overall.SURVEYS.fillna(0).astype(int)
    rows=[]
    for _,r in overall.iterrows():
        if pd.isna(r.RIDERSHIP):
            kind='Ridership unavailable';action='Fix missing APC boarding inputs or missing route/direction/time coverage'
        elif r.RIDERSHIP>1e-8 and r.SURVEYS==0:
            kind='Ridership without surveys';action='Review survey coverage for this route, direction and time period'
        elif r.RIDERSHIP<=1e-8 and r.SURVEYS>0:
            kind='Surveys without ridership';action='Review APC boarding totals and survey route/direction/time coding'
        else:continue
        rows.append({'ISSUE':kind,**{k:r[k] for k in GROUP},'SURVEYS':int(r.SURVEYS),'RIDERSHIP':r.RIDERSHIP,'ACTION':action})
    cols=['ISSUE']+GROUP+['SURVEYS','RIDERSHIP','ACTION']
    return pd.DataFrame(rows,columns=cols).sort_values(['ISSUE']+GROUP).reset_index(drop=True)
