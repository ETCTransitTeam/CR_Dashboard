import pandas as pd
import numpy as np
import numpy as np
import os
from datetime import datetime
import warnings

warnings.filterwarnings("ignore")

def check_all_characters_present(df, columns_to_check):
    # Function to clean a string by removing underscores and square brackets and converting to lowercase
    def clean_string(s):
        return s.replace('_', '').replace('[', '').replace(']', '').replace(' ','').replace('#','').lower()

    # Clean and convert all column names in df to lowercase for case-insensitive comparison
    df_columns_lower = [clean_string(column) for column in df.columns]

    # Clean and convert the columns_to_check list to lowercase for case-insensitive comparison
    columns_to_check_lower = [clean_string(column) for column in columns_to_check]

    # Use a list comprehension to filter columns
    matching_columns = [column for column in df.columns if clean_string(column) in columns_to_check_lower]

    return matching_columns


file_name = 'LACMTA_KINGElvis (3).xlsx'
df=pd.read_csv('elvislacmta2023obweekday_export_odbc(new2).csv')

# file_name = 'HRT_KINGElvis (3).xlsx'
# df=pd.read_csv('elvishrtva2023obweekday_export_odbc(new3).csv')

# file_name = '2023_GCRTA_KINGElvis (3).xlsx'
# df=pd.read_csv('elvisawsclevelandrta2023obweekday_export_odbc(new3).csv')

# file_name = 'BCT_KINGElvis (1).xlsx'
# df=pd.read_csv('elvisbroward2023obweekday_export_odbc(new2).csv')

# file_name = 'TRIANGLE_WAKE_KINGElvis.xlsx'
# df=pd.read_csv('elviswakenc2023obweekday_export_odbc(db).csv')

# file_name = 'VICTOR_KINGElvis.xlsx'
# df=pd.read_csv('elvisvictorvalley2023obweekday_export_odbc(db).csv')

if os.path.exists(file_name):
    elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')
else:
    print("File with name does not exists")

if file_name.split('_')[0].isdigit():
    file_first_name=file_name.split('_')[0]+'_'+file_name.split('_')[1]
else:
    file_first_name=file_name.split('_')[0]

elvis_date_check=['elvisdate']
elvis_date=check_all_characters_present(elvis_df,elvis_date_check)

df = df.merge(elvis_df[[elvis_date[0], 'id', 'Final_Usage']], on='id', how='left')

df=df[df['Final_Usage'].str.lower()=='use']

def age_split(x):
    if isinstance(x, str):  # Check if x is a string
        data = x.split(' ')
        for value in data:
            if value.isnumeric():
                return int(value)
    elif isinstance(x, float) or isinstance(x, int):
        return x
    return 0 

age_column_checks=['yourage']
age_column=check_all_characters_present(df,age_column_checks)

def calculate_age(birth_year):
    if birth_year == 0 or birth_year == 0.0:
        return 0  # Return None for an empty value
    current_year = datetime.now().year
    age = current_year - birth_year
    return age

if age_column:
    if df[age_column[0]].isna().all():
        age_column_check=['yearborn']
        age_column=check_all_characters_present(df,age_column_check)
        if age_column:
            if df[age_column[0]].isna().all():
                 df['AGE']=0
            else:
                df['AGE']=df[age_column[0]].apply(calculate_age)
        else:
             df['AGE']=0
    else:
        df['AGE']=df[age_column[0]].apply(age_split)
else:
    df['AGE']=0

df['student_od_flag']=None

student_od_flag_columns_checks=['originplacetypecode','destinplacetypecode','studentstatuscode']
student_od_flag_columns=check_all_characters_present(df,student_od_flag_columns_checks)
student_od_flag_columns.sort()

if len(student_od_flag_columns)==3:
    student_od_flag_check=(
        (
            ( # student_od_flag_columns[0] : 'DESTIN_PLACE_TYPECode' =5|6 : 'College / university'|'SChool (K-12)' 
                (df[student_od_flag_columns[0]] == '5') | (df[student_od_flag_columns[0]] == 5)
            )|(
                (df[student_od_flag_columns[0]] == '6') | (df[student_od_flag_columns[0]] == 6)
            )|
            # student_od_flag_columns[1] : 'ORIGIN_PLACE_TYPECode'=5|6 : 'College / university'|'SChool (K-12)'
            ((df[student_od_flag_columns[1]] == '5') | (df[student_od_flag_columns[1]] == 5))|
            ((df[student_od_flag_columns[1]] == '6') | (df[student_od_flag_columns[1]] == 6))
        )
        &(  # student_od_flag_columns[2] : 'STUDENT_STATUSCode'=1 : 'Not a Student'
            (df[student_od_flag_columns[2]] == '1') | (df[student_od_flag_columns[2]] == 1)
        )
    )

    df['student_od_flag']=np.where(student_od_flag_check,1,0)
else:
    #Any of the column student_od_flag_columns not present
    df['student_od_flag']=0

df['workplace_od_flag']=None

workplace_od_flag_columns_checks=['originplacetypecode','destinplacetypecode','employmentstatuscode']
workplace_od_flag_columns=check_all_characters_present(df,workplace_od_flag_columns_checks)
workplace_od_flag_columns.sort()

if len(workplace_od_flag_columns)==3:

    workplace_od_flag_check = (
        ( #workplace_od_flag_columns[2] : 'ORIGIN_PLACE_TYPECode'=1:"Your Usual Workplace"
            (df[workplace_od_flag_columns[2]] == '1') | (df[workplace_od_flag_columns[2]] == 1)|
            #workplace_od_flag_columns[0] : 'DESTIN_PLACE_TYPECode' =1:"Your Usual Workplace"
            (df[workplace_od_flag_columns[0]] == '1') | (df[workplace_od_flag_columns[0]] == 1)
        )
        &
         ( #workplace_od_flag_columns[1] : 'EMPLOYMENT_STATUSCode'=1:"Full Time Employe"
            (df[workplace_od_flag_columns[1]] > 2)   
        )
    )
    
    df['workplace_od_flag']=np.where(workplace_od_flag_check,1,0)
else:
    #Any of the column workplace_od_flag_columns not present
    df['workplace_od_flag']=0
    df['OLD_K12_STUDENT']=None
df['YOUNG_OLD_COLLEGE_STUDENT']=None

if len(student_od_flag_columns)==3:
    old_k12_student_check = (
        ((df[student_od_flag_columns[0]] == '6') | (df[student_od_flag_columns[0]] == 6)) |
        ((df[student_od_flag_columns[1]] == '6') | (df[student_od_flag_columns[1]] == 6)) |
        ((df[student_od_flag_columns[2]] == '5') | (df[student_od_flag_columns[2]] == 5))
    ) & (
        (df['AGE'] >= 18) & (df['AGE'] != 0)
    )

    young_old_college_student_check = (
        # student_od_flag_columns[0] : 'DESTIN_PLACE_TYPECode'=5 : "college/university"
        ((df[student_od_flag_columns[0]] == '5') | (df[student_od_flag_columns[0]] == 5)) |
         # student_od_flag_columns[1] : 'ORIGIN_PLACE_TYPECode'=5 : "college/university"
        ((df[student_od_flag_columns[1]] == '5') | (df[student_od_flag_columns[1]] == 5)) |
        # student_od_flag_columns[2] : 'STUDENT_STATUSCode'=2|3: 'Full Time Cllege'|'Part Time Student'
        (((df[student_od_flag_columns[2]] == '2') | (df[student_od_flag_columns[2]] == 2)) |
        ((df[student_od_flag_columns[2]] == '3') | (df[student_od_flag_columns[2]] == 3))
        )
    ) & (
        ((df['AGE'] <= 16) | (df['AGE'] >= 65)) &(df['AGE']!=0)
    )

    df['YOUNG_OLD_COLLEGE_STUDENT']=np.where(young_old_college_student_check,1,0)
    df['OLD_K12_STUDENT']=np.where(old_k12_student_check,1,0)
else:
    df['YOUNG_OLD_COLLEGE_STUDENT']=0
    df['OLD_K12_STUDENT']=0

df['EMPLOYED_IN_HH_1']=None
df['EMPLOYED_IN_HH_2']=None

employed_hh_columns_checks=['employedinhhcode','hhsizecode']
employed_hh_columns=check_all_characters_present(df,employed_hh_columns_checks)
employed_hh_columns.sort()

employment_status_column_check=['employmentstatuscode']
employment_status_column=check_all_characters_present(df,employment_status_column_check)

if len(employed_hh_columns)==2 and employment_status_column:
    employed_hh_check1 = (
        (
            #employed_hh_columns[0] : 'EMPLOYED_IN_HHCode'
            (df[employed_hh_columns[0]] == 0) | (df[employed_hh_columns[0]] == '0')
        )
        &
        (
            (
                #workplace_od_flag_columns[1] : 'EMPLOYMENT_STATUSCode'
                (df[employment_status_column[0]] == '1') | (df[employment_status_column[0]] == 1)
            )
            |
            (
                (df[employment_status_column[0]] == '2') | (df[employment_status_column[0]] == 2)
            )
        )
    )

    employed_hh_check2 = (
        (
            #employed_hh_columns[0] : 'EMPLOYED_IN_HHCode'
            (df[employed_hh_columns[0]] == 1) | (df[employed_hh_columns[0]] == '1')
        )
        &
        (
            #employed_hh_columns[1] : HH_SIZECode'
            (df[employed_hh_columns[1]] == 1) | (df[employed_hh_columns[1]] == '1')
        )
        &
        ~(          #workplace_od_flag_columns[1] : 'EMPLOYMENT_STATUSCode'
            ( 
                (df[employment_status_column[0]] == '1') | (df[employment_status_column[0]] == 1)
            )
            |
            (
                (df[employment_status_column[0]] == '2') | (df[employment_status_column[0]] == 2)
            )
        )
    )

    df['EMPLOYED_IN_HH_1']=np.where(employed_hh_check1,1,0)
    df['EMPLOYED_IN_HH_2']=np.where(employed_hh_check2,1,0)

else:
    df['EMPLOYED_IN_HH_1']=0
    df['EMPLOYED_IN_HH_2']=0

df['EMPLOYED_IN_HH_GREATER_THAN_HH_SIZE']=None
df['TRAVEL_WITH_HH_GREATER_THAN_HH_SIZE']=None
if len(employed_hh_columns)==2:
    employed_in_hh_greater_than_hh_size_check=(
        (  #employed_hh_columns[0] : 'EMPLOYED_IN_HHCode'
            #employed_hh_columns[1] : HH_SIZECode'
            df[employed_hh_columns[0]]>df[employed_hh_columns[1]]
        )
    )
    df['EMPLOYED_IN_HH_GREATER_THAN_HH_SIZE']=np.where(employed_in_hh_greater_than_hh_size_check,1,0)
else:
    df['EMPLOYED_IN_HH_GREATER_THAN_HH_SIZE']=0


travel_hh_columns_checks=['travelhh']
travel_hh_columns=check_all_characters_present(df,travel_hh_columns_checks)

if travel_hh_columns and len(employed_hh_columns) == 2:
    travel_hh_check=(
    (   #employed_hh_columns[1] : 'HH_SIZECode'
        #travel_hh_columns[0] : 'TRAVEL_HH'
        df[travel_hh_columns[0]]>df[employed_hh_columns[1]]
    )
    )
    df['TRAVEL_WITH_HH_GREATER_THAN_HH_SIZE']=np.where(travel_hh_check,1,0)
else:
    #TRAVEL_HH column not present
    df['TRAVEL_WITH_HH_GREATER_THAN_HH_SIZE']=0

count_vh_hh_flag_column_checks=['countvhhhcode','usedvehtripcode']
count_vh_hh_flag_columns=check_all_characters_present(df,count_vh_hh_flag_column_checks)
count_vh_hh_flag_columns.sort()

if len(count_vh_hh_flag_columns)==2:
    count_vh_hh_check=(
        (   #count_vh_hh_flag_columns[0] : "COUNT_VH_HHCode"
            df[count_vh_hh_flag_columns[0]]==0
        )&
        (   #count_vh_hh_flag_columns[1] : "USED_VEH_TRIPCode"
             df[count_vh_hh_flag_columns[1]].notnull()
        )
    )
    df['COUNT_VH_HH_FLAG']=np.where(count_vh_hh_check,1,0)
else:
    #ANy of the columns ['COUNT_VH_HHCode', 'USED_VEH_TRIPCode'] not present
    df['COUNT_VH_HH_FLAG']=0

extend_columns_checks=['employedinhh','hhsize','destinplacetype','originplacetype','studentstatus','employmentstatuscode','employmentstatus']
extend_columns=check_all_characters_present(df,extend_columns_checks)
extend_columns.sort()
extend_columns

dl_column_checks=['havedl', 'driverlicense', 'driverlicensecode']
dl_column=check_all_characters_present(df,dl_column_checks)

if dl_column and 'AGE' in df.columns:
    dl_vals = df[dl_column[0]].astype(str).str.strip().str.lower()
    has_dl = (dl_vals == 'yes') | (dl_vals == '1')
    young_driver_check = has_dl & (df['AGE'] <= 16)
    df['YOUNG_DRIVER'] = np.where(young_driver_check, 1, 0)
else:
    df['YOUNG_DRIVER'] = 0

df['FARE_STUDENT_FLAG']=None

fare_columns_checks=['typefare','typeoffare','faretype','faretypecode']
fare_columns=check_all_characters_present(df,fare_columns_checks)

student_status_column_check=['studentstatuscode']
student_status_column=check_all_characters_present(df,student_status_column_check)

fare_employment_column_checks=['employmentstatuscode']
fare_employment_column=check_all_characters_present(df,fare_employment_column_checks)

df['FARE_EMPLOYMENT_FLAG']=None

fare_employment_column_checks=['employmentstatuscode']
fare_employment_column=check_all_characters_present(df,fare_employment_column_checks)

if fare_columns and fare_employment_column:
    employment_col = fare_employment_column[0]
    fare_employment_flag_check=(
        (df[fare_columns[0]].astype(str).str.lower().str.contains('employee|metroworks', case=False, na=False))
        & (pd.to_numeric(df[employment_col], errors='coerce').fillna(0) > 2)
    )
    df['FARE_EMPLOYMENT_FLAG']=np.where(fare_employment_flag_check,1,0)
else:
    df['FARE_EMPLOYMENT_FLAG']=0

if fare_columns and student_status_column:
    student_fare_type_checks = (
    (df[fare_columns[0]] == 'U-Pass or CMSD ID') |
    (df[fare_columns[0]] == 'Go Smester') |
    (df[fare_columns[0]] == 'Student Freedom Pass') |
    (df[fare_columns[0]] == '1-Trip, 2-Trip, or 5-Trip Fare Card') |
    (df[fare_columns[0]] == 'All-Day Pass') |
    (df[fare_columns[0]] == '1-Ride Ticket or cash fare')
    )
    #student_status_column : 'STUDENT_STATUSCODE'
    df['FARE_STUDENT_FLAG'] = (student_fare_type_checks & (df[student_status_column[0]] == 1)).astype(int)
else:
    df['FARE_STUDENT_FLAG'] = 0

df['SUM_ALL_CHECKS']=np.where(df[['student_od_flag','workplace_od_flag','OLD_K12_STUDENT','YOUNG_OLD_COLLEGE_STUDENT','EMPLOYED_IN_HH_1','EMPLOYED_IN_HH_2','EMPLOYED_IN_HH_GREATER_THAN_HH_SIZE','TRAVEL_WITH_HH_GREATER_THAN_HH_SIZE','COUNT_VH_HH_FLAG','YOUNG_DRIVER','FARE_STUDENT_FLAG','FARE_EMPLOYMENT_FLAG']].any(axis=1),1,0)

# df=df[df['AGE']!=0]

output_columns = ['id', *employed_hh_columns, *student_od_flag_columns, *extend_columns]
if dl_column:
    output_columns.append(dl_column[0])
if age_column:
    output_columns.append(age_column[0])
output_columns.extend(fare_columns)
output_columns.extend([
    'student_od_flag', 'workplace_od_flag', 'OLD_K12_STUDENT', 'YOUNG_OLD_COLLEGE_STUDENT',
    'EMPLOYED_IN_HH_1', 'EMPLOYED_IN_HH_2', 'EMPLOYED_IN_HH_GREATER_THAN_HH_SIZE',
    'TRAVEL_WITH_HH_GREATER_THAN_HH_SIZE', 'COUNT_VH_HH_FLAG', 'YOUNG_DRIVER',
    'FARE_STUDENT_FLAG', 'FARE_EMPLOYMENT_FLAG', 'SUM_ALL_CHECKS',
])
seen: set[str] = set()
final_columns: list[str] = []
for col in output_columns:
    if col not in seen and col in df.columns:
        seen.add(col)
        final_columns.append(col)
df = df[final_columns]
df.drop_duplicates(subset=['id'],keep='first',inplace=True)

df[df['SUM_ALL_CHECKS']==1].to_csv(f'{file_first_name}_DemoGraphic_Checks(01).csv',index=False)
print(f'{file_first_name}_DemoGraphic_Checks.csv SuccessFully Created')