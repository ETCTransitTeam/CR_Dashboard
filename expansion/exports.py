from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import json, math
import pandas as pd
import numpy as np
import xlsxwriter
from expansion.engine import KEY, text, text_columns

def export_stamp(now=None):
    current=now if now is not None else datetime.now(timezone.utc)
    return current.astimezone(ZoneInfo('America/Chicago')).strftime('%Y%m%d%H%M%S')

def stamped_filename(name,stamp=None):
    path=Path(name)
    return f'{path.stem}_{stamp or export_stamp()}{path.suffix}'

def excel_view(result):
    d=result['cells'];v=pd.DataFrame()
    headers=['TABLE8.ROUTE_SURVEY','TABLE8.FINAL_ROUTE_DIRECTION','TABLE8.TIME_PERIOD','TABLE8.BOARDING_SEG','TABLE8.ALIGHTING_SEG']
    for h,k in zip(headers,KEY):v[h]=d[k]
    v['TABLE6_RIDERSHIP']=d.RIDERSHIP_USED
    v['TABLE7.Count']=d.SURVEYS.fillna(0)
    v['TABLE8.WEIGHT-1']=d.FINAL_WEIGHT
    v['NULL_MERGE']=d.NULL_MERGE
    daytypes={text(x) for x in result['survey_input'].get('DATE_TYPE',[]) if text(x)}
    day=next(iter(daytypes),'Weekday')
    v['ROUTE_SURVEY_TIME_PERIOD_BOARDING_SEG_ALIGHTING_SEG']=d.ROUTE_SURVEY+'_'+d.TIME_PERIOD+'_'+day+'_'+d.BOARDING_SEG+'_'+d.ALIGHTING_SEG
    v['EXP']=d.FINAL_EXPANDED
    v['TABLE6_RIDERSHIP TOTAL']=d.F_TOTAL
    v['TABLE6_RIDERSHIP ERROR']=d.FLAG_RIDERSHIP
    v['Normalize']=d.RIDERSHIP_USED/d.F_TOTAL*d.APC_TOTAL
    v['RIDERSHIP']=d.APC_TOTAL
    v['EXPANDED']=d.K_TOTAL
    v['FLAG EXPANDED']=d.FLAG_EXPANDED
    v['NORM']=d.NORMALIZED_RIDERSHIP
    v['COLLAPSE_CORRECT']=d.FINAL_NORMALIZED_WEIGHT
    v['TOD']=d.APC_TOTAL/d.SURVEY_TOTAL
    v['HWFACTOR']=d.HWFACTOR
    v['MERGE TOD']=None
    v['ROUTE']=d.ROUTE
    v['NO RIDERSHIP']=d.NO_RIDERSHIP
    v['SURVEYS SEGMENT']=d.OD_COUNT
    v['FLAG SEGMENT']=d.FLAG_CELL_SURVEYS
    v['SURVEYS EXPORT']=d.SURVEY_TOTAL
    v['SURVEYS OD TIME']=d.OD_TOTAL
    v['FLAG TIME SURVEYS']=d.FLAG_TIME_SURVEYS
    v['ROUTE_NO_DIRECTION']=d.ROUTE
    v['Total Ridership Expanded']=d.ROUTE_EXPANDED
    v['Total Ridership APC']=d.ROUTE_APC
    v['FLAG ROUTE RIDERSHIP']=d.FLAG_ROUTE_RIDERSHIP
    v['Total Surveys Expanded']=d.ROUTE_SURVEYS
    v['Total Surveys OD']=d.ROUTE_OD
    v['FLAG ROUTE SURVEYS']=d.FLAG_ROUTE_SURVEYS
    return v

def weighted_surveys(r):
    od=r['survey_input'].copy();d=r['cells']
    for k in KEY:od[k]=od[k].map(text)
    return od.merge(d[KEY+['RAW_WEIGHT','FINAL_WEIGHT','FINAL_NORMALIZED_WEIGHT','WEIGHT_SOURCE','POOL']],on=KEY,how='left',validate='many_to_one')

def clean_frame(d):
    d=d.copy()
    for c in text_columns(d):
        d[c]=d[c].map(lambda v:"'"+v if isinstance(v,str) and v.startswith(('=','+','-','@')) else v)
    return d.replace([np.inf,-np.inf],np.nan)

def write_workbook(frames,output):
    # Portable runtime: strings are never interpreted as formulas or URLs.
    with xlsxwriter.Workbook(str(output),{'strings_to_formulas':False,'strings_to_urls':False}) as book:
        head=book.add_format({'font_name':'Arial','font_size':10,'bold':True,'bg_color':'#20354D','font_color':'white','text_wrap':True,'valign':'vcenter'})
        body=book.add_format({'font_name':'Arial','font_size':10,'valign':'vcenter'})
        decimal=book.add_format({'font_name':'Arial','font_size':10,'num_format':'#,##0.000000'})
        apc_number=book.add_format({'font_name':'Arial','font_size':10,'num_format':'#,##0.00'})
        integer=book.add_format({'font_name':'Arial','font_size':10,'num_format':'#,##0'})
        for name,frame in frames.items():
            d=clean_frame(frame);sheet=book.add_worksheet(name);sheet.hide_gridlines(2);sheet.freeze_panes(1,0);sheet.set_row(0,36)
            for col,label in enumerate(d.columns):
                sheet.write_string(0,col,str(label),head)
                lengths=[len(str(label))]+[len(v) for v in d.iloc[:500,col] if isinstance(v,str)]
                sheet.set_column(col,col,min(75,max(16,max(lengths)+3)))
            for row,values in enumerate(d.itertuples(index=False,name=None),1):
                for col,value in enumerate(values):
                    if value is None or pd.isna(value):sheet.write_blank(row,col,None,body)
                    elif isinstance(value,(bool,np.bool_)):sheet.write_boolean(row,col,bool(value),body)
                    elif isinstance(value,(int,float,np.number)):
                        fmt=integer if isinstance(value,(int,np.integer)) else apc_number if name in ['APC','Updated APC'] else decimal
                        sheet.write_number(row,col,float(value),fmt)
                    else:sheet.write_string(row,col,str(value),body)
            if len(d):sheet.autofilter(0,0,len(d),len(d.columns)-1)
    return Path(output)

def export_run(r,folder,xlsx=True,stamp=None):
    stamp=stamp or export_stamp();folder=Path(folder);folder.mkdir(parents=True,exist_ok=True)
    summary=pd.DataFrame([{'Metric':k.replace('_',' ').capitalize(),'Value':v} for k,v in r['info'].items()])
    frames={'Run summary':summary,'Areas to fix':r['areas_to_fix'],'Updated APC':r['updated_apc'],'APC merge log':r['apc_merge_log'],'EXPORT':excel_view(r),'Weighted surveys':weighted_surveys(r),'Route-time totals':r['groups'],'Adjustments':r['adjustments'],'Checks':r['checks']}
    for name,d in frames.items():clean_frame(d).to_csv(folder/stamped_filename(name+'.csv',stamp),index=False,encoding='utf-8-sig')
    (folder/stamped_filename('Run summary.json',stamp)).write_text(json.dumps(r['info'],indent=2),encoding='utf-8')
    if xlsx:write_workbook(frames,folder/stamped_filename('Expansion results.xlsx',stamp))
    return folder

def export_fix_workbook(r,folder,stamp=None):
    stamp=stamp or export_stamp();folder=Path(folder);folder.mkdir(parents=True,exist_ok=True)
    frames={'Areas to fix':r['areas_to_fix'],'APC':r['updated_apc'],'APC merge log':r['apc_merge_log']}
    return write_workbook(frames,folder/stamped_filename('Areas to fix and Updated APC.xlsx',stamp))
