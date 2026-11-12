from pathlib import Path
import json, math, csv
import numpy as np
import pandas as pd
import openpyxl

KEY=['ROUTE_SURVEY','FINAL_ROUTE_DIRECTION','TIME_PERIOD','BOARDING_SEG','ALIGHTING_SEG']
GROUP=KEY[:3]
VERSION='1.1.0'

def text_columns(frame):
    # pandas 2 rejects include=['str']; pandas 3 uses that label for string columns.
    columns = list(frame.select_dtypes(include=['object']).columns)
    for dtype in ('string', 'str'):
        try:
            extra = frame.select_dtypes(include=[dtype]).columns
        except (TypeError, ValueError):
            continue
        for column in extra:
            if column not in columns:
                columns.append(column)
    return columns

def text(v):
    if v is None or pd.isna(v): return ''
    if isinstance(v,(float,int)) and float(v).is_integer(): return str(int(v))
    return str(v).strip()

def read_workbook(path):
    w=openpyxl.load_workbook(path,read_only=True,data_only=True)
    result={}
    for name in ['O2O','OD','APC','SAMPLING']:
        if name not in w.sheetnames: raise ValueError('Missing required worksheet: '+name)
        it=w[name].values;headers=[text(v) for v in next(it)]
        # Only named columns are read. Formula helper headers are not inputs.
        good=[(i,h) for i,h in enumerate(headers) if h]
        rows=[{h:row[i] if i<len(row) else None for i,h in good} for row in it if any(v is not None for v in row)]
        result[name]=pd.DataFrame(rows)
    w.close()
    return result

def prepare(tables,allow_incomplete_apc=False):
    result={}
    for name,cols in [('O2O',KEY),('OD',KEY),('APC',['ROUTE_SURVEYED','FINAL_ROUTE_DIRECTION','Segment']+[f'{i}_{m}' for i in range(7) for m in ['ON','OFF']]),('SAMPLING',['ROUTE_SURVEYED','FINAL_ROUTE_DIRECTION','SAMPLING_RATE'])]:
        d=tables[name].copy()
        missing=set(cols)-set(d.columns)
        if missing: raise ValueError(name+' missing columns: '+', '.join(sorted(missing)))
        route='ROUTE_SURVEY' if name in ['O2O','OD'] else 'ROUTE_SURVEYED'
        d=d[d[route].notna() & (d[route].map(text)!='')].copy()
        if name=='APC':d=d[d.Segment.map(text)!='-'].copy()
        stringcols=KEY if name in ['O2O','OD'] else ['ROUTE_SURVEYED','FINAL_ROUTE_DIRECTION']+(['Segment'] if name=='APC' else [])
        for c in stringcols:
            d[c]=d[c].map(text)
            if (d[c]=='').any():raise ValueError(f'{name}: blank {c}')
        if name in ['O2O','OD']:
            for c in KEY[2:]:
                if not d[c].str.fullmatch(r'\d+').all():raise ValueError(name+': '+c+' must contain nonnegative integers')
            for c in ['BOARDING_SEG','ALIGHTING_SEG']:
                if (d[c].astype(int)<1).any():raise ValueError(name+': segments must start at 1')
            if 'DATE_TYPE' in d:
                dates={text(v) for v in d.DATE_TYPE if text(v)}
                if len(dates)>1:raise ValueError('Separate day types into separate runs; the original model does not group by DATE_TYPE.')
        if name=='APC':
            if not d.Segment.str.fullmatch(r'[1-9]\d*').all():raise ValueError('APC: Segment must be a positive integer')
            if d.duplicated(['ROUTE_SURVEYED','FINAL_ROUTE_DIRECTION','Segment']).any():raise ValueError('Duplicate APC route/direction/segment keys')
            for c in [f'{i}_{m}' for i in range(7) for m in ['ON','OFF']]:
                d[c]=pd.to_numeric(d[c],errors='raise')
                if np.isinf(d[c]).any() or (d[c]<0).any() or (not allow_incomplete_apc and d[c].isna().any()):raise ValueError('APC: missing, negative, or non-finite '+c)
        if name=='SAMPLING':
            if d.duplicated(['ROUTE_SURVEYED','FINAL_ROUTE_DIRECTION']).any():raise ValueError('Duplicate sampling route/direction keys')
            d.SAMPLING_RATE=pd.to_numeric(d.SAMPLING_RATE,errors='raise')
            if not d.SAMPLING_RATE.between(0,100,inclusive='right').all():raise ValueError('Sampling rates must be >0 and <=100 (percentage points)')
        result[name]=d
    daytypes={text(v) for name in ['OD','O2O'] if 'DATE_TYPE' in result[name] for v in result[name].DATE_TYPE if text(v)}
    if len(daytypes)>1:raise ValueError('O2O and OD day types differ. Run one common day type at a time.')
    a=result['APC'];valid=set(zip(a.ROUTE_SURVEYED,a.FINAL_ROUTE_DIRECTION,a.Segment))
    for name in ['OD','O2O']:
        d=result[name]
        for c in ['BOARDING_SEG','ALIGHTING_SEG']:
            bad=[(r,n,s) for r,n,s in zip(d.ROUTE_SURVEY,d.FINAL_ROUTE_DIRECTION,d[c]) if (r,n,s) not in valid]
            if bad:raise ValueError(f'{name}: {len(bad)} unmatched APC {c} keys, first: {bad[0]}')
    s=result['SAMPLING'];sampling=set(zip(s.ROUTE_SURVEYED,s.FINAL_ROUTE_DIRECTION))
    return result

def expand(tables,excel_sample_rate=4.25,project_rate_override=False,allow_incomplete_apc=False):
    if not 0<excel_sample_rate<=100:raise ValueError('Excel sampling rate must be >0 and <=100')
    t=prepare(tables,allow_incomplete_apc);o=t['O2O'];od=t['OD'];a=t['APC'];s=t['SAMPLING']
    raw=o.groupby(KEY,dropna=False).size().rename('TABLE_1').reset_index()
    counts=od.groupby(KEY,dropna=False).size().rename('SURVEYS').reset_index()
    rawkeys=set(map(tuple,raw[KEY].values));od_lookup=counts.set_index(KEY).SURVEYS.to_dict()
    candidates=[]
    # Dynamic O2O_MISSING: enumerate every route-specific segment, rather than fixed fill columns.
    # Its left join to OD deliberately duplicates the dummy once per matching survey.
    for (r,n),g in a.groupby(['ROUTE_SURVEYED','FINAL_ROUTE_DIRECTION'],sort=False):
        segs=sorted(g.Segment.astype(int).tolist())
        if segs!=list(range(1,max(segs)+1)):raise ValueError(f'APC segments must be contiguous from 1: {r}')
        for period in ['1','2','3','4']:
            for b in segs:
                for al in segs:
                    key=(r,n,period,str(b),str(al))
                    if al>=b and key not in rawkeys:candidates.append([*key,max(1,od_lookup.get(key,0))])
    dummy=pd.DataFrame(candidates,columns=KEY+['TABLE_1'])
    cells=pd.concat([raw,dummy],ignore_index=True).groupby(KEY,as_index=False).TABLE_1.sum()
    cells['TABLE_1']=pd.to_numeric(cells.TABLE_1,errors='raise')
    apc=a.rename(columns={'ROUTE_SURVEYED':'ROUTE_SURVEY'})
    on=apc.melt(id_vars=['ROUTE_SURVEY','FINAL_ROUTE_DIRECTION','Segment'],value_vars=[f'{i}_ON' for i in range(7)],var_name='TIME_PERIOD',value_name='ON')
    off=apc.melt(id_vars=['ROUTE_SURVEY','FINAL_ROUTE_DIRECTION','Segment'],value_vars=[f'{i}_OFF' for i in range(7)],var_name='TIME_PERIOD',value_name='OFF')
    on.TIME_PERIOD=on.TIME_PERIOD.str.split('_').str[0];off.TIME_PERIOD=off.TIME_PERIOD.str.split('_').str[0]
    totals=on.groupby(GROUP,as_index=False).ON.sum().rename(columns={'ON':'APC_TOTAL'})
    missing_apc=[];invalid_groups=set()
    for _,row in a.iterrows():
        for c in [f'{i}_{mode}' for i in range(7) for mode in ['ON','OFF']]:
            if pd.isna(row[c]):
                period,mode=c.split('_');invalid_groups.add((row.ROUTE_SURVEYED,row.FINAL_ROUTE_DIRECTION,period))
                missing_apc.append({'ROUTE_SURVEY':row.ROUTE_SURVEYED,'FINAL_ROUTE_DIRECTION':row.FINAL_ROUTE_DIRECTION,'TIME_PERIOD':period,'BOARDING_SEG':row.Segment if mode=='ON' else '', 'ALIGHTING_SEG':row.Segment if mode=='OFF' else '', 'SOURCE_COLUMN':c})
    totals.loc[[tuple(row) in invalid_groups for row in totals[GROUP].values],'APC_TOTAL']=np.nan
    cells=cells.merge(totals,on=GROUP,how='left',validate='many_to_one')
    cells['TABLE_3']=cells.TABLE_1/cells.groupby(GROUP).TABLE_1.transform('sum')*cells.APC_TOTAL
    cells=cells.merge(on.rename(columns={'Segment':'BOARDING_SEG'}),on=GROUP+['BOARDING_SEG'],how='left',validate='many_to_one')
    cells=cells.merge(off.rename(columns={'Segment':'ALIGHTING_SEG'}),on=GROUP+['ALIGHTING_SEG'],how='left',validate='many_to_one')
    value=cells.TABLE_3.copy();iterations=[]
    for step in range(1,11):
        cells['_value']=value
        den=cells.groupby(GROUP+['ALIGHTING_SEG'])._value.transform('sum')
        with np.errstate(divide='ignore',invalid='ignore'): al=cells.OFF/den*value
        if step>1:al=al.fillna(0)
        cells['_value']=al
        den=cells.groupby(GROUP+['BOARDING_SEG'])._value.transform('sum')
        with np.errstate(divide='ignore',invalid='ignore'):value=cells.ON/den*al
        if step>1:value=value.fillna(0)
        if np.isinf(value).any():raise ValueError('Positive APC target has zero expansion support; infinite fitted value.')
        iterations.append((al.copy(),value.copy()))
    cells['TABLE6_RIDERSHIP']=value
    cells.loc[[tuple(row) in invalid_groups for row in cells[GROUP].values],'TABLE6_RIDERSHIP']=np.nan
    out=cells[KEY+['TABLE6_RIDERSHIP']].merge(counts,on=KEY,how='outer',validate='one_to_one')
    out=out.merge(totals,on=GROUP,how='left',validate='many_to_one')
    rates=s.rename(columns={'ROUTE_SURVEYED':'ROUTE_SURVEY'})[['ROUTE_SURVEY','FINAL_ROUTE_DIRECTION','SAMPLING_RATE']]
    out=out.merge(rates,on=KEY[:2],how='left',validate='many_to_one')
    out['ROUTE_SAMPLING_RATE']=out.SAMPLING_RATE
    if project_rate_override:out['SAMPLING_RATE']=float(excel_sample_rate)
    out['WEIGHT']=out.TABLE6_RIDERSHIP/out.SURVEYS
    out['MAX_WEIGHT_FACTOR']=300/out.SAMPLING_RATE
    # Power Query replaces null weight with zero. Preserve missing survey counts separately.
    out['NULL_MERGE']=((out.WEIGHT.fillna(0)>out.MAX_WEIGHT_FACTOR)|np.isinf(out.WEIGHT)).astype(int)
    out['EXPANDED']=out.SURVEYS*out.WEIGHT
    for c in ['WEIGHT','EXPANDED']:
        out.loc[out.SURVEYS.isna(),c]=0
    out['REPRESENTED_RIDERSHIP']=out.TABLE6_RIDERSHIP.where(out.SURVEYS.fillna(0)>0,0)
    represented=out.groupby(GROUP).REPRESENTED_RIDERSHIP.transform('sum')
    out['NORMALIZED_RIDERSHIP']=out.REPRESENTED_RIDERSHIP/represented*out.APC_TOTAL
    out['NORMALIZED_WEIGHT']=out.NORMALIZED_RIDERSHIP/out.SURVEYS
    out.loc[out.SURVEYS.isna(),'NORMALIZED_WEIGHT']=np.nan
    out['ROUTE']=out.ROUTE_SURVEY.str.rsplit('_',n=1).str[0]
    issues=[]
    def issue(kind,row,actual=None,expected=None,detail=''):
        issues.append({'CHECK':kind,**{k:row.get(k,'') for k in KEY},'ACTUAL':actual,'EXPECTED':expected,'DETAIL':detail})
    for _,row in out.iterrows():
        if pd.isna(row.TABLE6_RIDERSHIP):issue('OD cell missing from expansion',row,row.SURVEYS,None,'Survey cell was not produced by the expansion matrix')
        if row.TABLE6_RIDERSHIP>1e-8 and pd.isna(row.SURVEYS):issue('Ridership without survey',row,row.TABLE6_RIDERSHIP,0,'Positive fitted ridership has no matching OD survey')
        if row.SURVEYS>0 and row.TABLE6_RIDERSHIP==0:issue('Survey without ridership',row,row.SURVEYS,0)
        if pd.isna(row.SAMPLING_RATE):issue('Missing sampling rate',row,None,None,'High-weight check unavailable')
        if row.NULL_MERGE:issue('High weight (route sampling)',row,row.WEIGHT,row.MAX_WEIGHT_FACTOR)
        rg=out[(out.ROUTE_SURVEY==row.ROUTE_SURVEY)&(out.FINAL_ROUTE_DIRECTION==row.FINAL_ROUTE_DIRECTION)]
        if row.WEIGHT>300/excel_sample_rate and rg.WEIGHT.max()!=rg.WEIGHT.min():issue('High weight (Excel fixed rate)',row,row.WEIGHT,300/excel_sample_rate)
    group=out.groupby(GROUP,as_index=False).agg(APC_TOTAL=('APC_TOTAL','first'),FITTED_RIDERSHIP=('TABLE6_RIDERSHIP','sum'),REPRESENTED_RIDERSHIP=('REPRESENTED_RIDERSHIP','sum'),OD_SURVEYS=('SURVEYS','sum'))
    # Recount raw OD independently, including cells absent from the fitted matrix.
    direct=od.groupby(GROUP).size().to_dict()
    for _,row in group.iterrows():
        k=tuple(row[c] for c in GROUP)
        for check,col in [('Fitted ridership vs APC','FITTED_RIDERSHIP'),('Expanded ridership vs APC','REPRESENTED_RIDERSHIP')]:
            if abs(row[col]-row.APC_TOTAL)>1e-5:issue(check,row,row[col],row.APC_TOTAL)
        if row.OD_SURVEYS!=direct.get(k,0):issue('Survey count by time period',row,row.OD_SURVEYS,direct.get(k,0))
    # Alighting fit is observed after exactly ten iterations, never silently iterated to convergence.
    fitted=cells.groupby(GROUP+['ALIGHTING_SEG'],as_index=False).TABLE6_RIDERSHIP.sum().merge(off.rename(columns={'Segment':'ALIGHTING_SEG'}),on=GROUP+['ALIGHTING_SEG'],how='outer')
    for _,row in fitted.iterrows():
        actual=0 if pd.isna(row.TABLE6_RIDERSHIP) else row.TABLE6_RIDERSHIP
        if abs(actual-row.OFF)>1e-5:issue('Alighting residual after 10 iterations',row,actual,row.OFF,'Diagnostic beyond the existing Excel checks')
    result_issues=pd.DataFrame(issues,columns=['CHECK']+KEY+['ACTUAL','EXPECTED','DETAIL'])
    out=out.sort_values(KEY).reset_index(drop=True)
    info={'version':VERSION,'iterations':10,'o2o_records':len(o),'od_records':len(od),'dummy_records':int(dummy.TABLE_1.sum()),'expansion_cells':len(out),'apc_ridership':float(totals.APC_TOTAL.sum()),'fitted_ridership':float(out.TABLE6_RIDERSHIP.sum()),'represented_ridership':float(out.REPRESENTED_RIDERSHIP.sum()),'issue_rows':len(issues),'excel_sampling_rate_percent':excel_sample_rate,'guideline_weight_factor':300/excel_sample_rate,'missing_apc_values':len(missing_apc),'totals_are_partial':bool(missing_apc),'weight_limit_source':'Project sampling percentage' if project_rate_override else 'Route SAMPLING table'}
    return {'cells':out,'checks':result_issues,'groups':group,'info':info,'survey_input':od,'missing_apc':missing_apc,'stages':{'TABLE1':cells[KEY+['TABLE_1']],'TABLE2&3':cells[KEY+['TABLE_3']],'TABLE6':cells[KEY+['TABLE6_RIDERSHIP']]},'iterations':iterations}

def save_csv(result,folder):
    folder=Path(folder);folder.mkdir(parents=True,exist_ok=True)
    for key,name in [('cells','Expansion'),('checks','Checks'),('groups','Route-time totals')]:
        # Neutralize Excel formula injection in all exported text values.
        d=result[key].copy()
        for c in text_columns(d):
            d[c]=d[c].map(lambda v: "'"+v if isinstance(v,str) and v.startswith(('=','+','-','@')) else v)
        d.to_csv(folder/(name+'.csv'),index=False,encoding='utf-8-sig')
    od=result['survey_input'];cells=result['cells']
    weighted=od.merge(cells[KEY+['WEIGHT','NORMALIZED_WEIGHT']],on=KEY,how='left',validate='many_to_one',suffixes=('_INPUT',''))
    for c in text_columns(weighted):
        weighted[c]=weighted[c].map(lambda v:"'"+v if isinstance(v,str) and v.startswith(('=','+','-','@')) else v)
    weighted.to_csv(folder/'Weighted surveys.csv',index=False,encoding='utf-8-sig')
    (folder/'Run summary.json').write_text(json.dumps(result['info'],indent=2),encoding='utf-8')
    return folder
