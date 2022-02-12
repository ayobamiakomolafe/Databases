

'Path to the data'
Json_data='user_data.json'
Xml_data='user_data.xml'
Csv_data='user_data.csv'
Txt_data='user_data.txt'

'IMPORT NEEDED LIBRARIES'
import pandas as pd
'Import xml.etree.ElementTree to read XML data'
import xml.etree.ElementTree as ET

'Read csv_data to a pandas Dataframe'
df_csv_data=pd.read_csv(Csv_data)
print("csv_data to a pandas Dataframe: \n ", df_csv_data.head())

print('\n')

"Read Json_data to pandas Dataframe"
df_json_data=pd.read_json(Json_data)
print('Json_data to pandas Dataframe: \n',   df_json_data.head())

print('\n')

"Read Xml_data to pandas Dataframe"
tree = ET.parse(Xml_data)
root = tree.getroot()
xml_data_cols = ['lastname', 'firstname', 'address_postcode', 'age', 'commute_distance', 'company', 'dependants','marital_status','pension','retired','salary','sex']
df_xml_data = pd.DataFrame(columns=xml_data_cols)
for user in root.findall('user'):
    df_xml_data = df_xml_data.append(
        pd.Series([user.get('lastName'), user.get('firstName'), user.get('address_postcode'),user.get('age'), user.get('commute_distance'), user.get('company'),user.get('dependants'),\
                   user.get('marital_status'),user.get('pension'),user.get('retired'),user.get('salary'), user.get('sex' )], index=xml_data_cols), ignore_index=True)
print('Xml_data to pandas Dataframe \n', df_xml_data.head())

print('\n')

"Create new column---Name"
df_csv_data['Name']=df_csv_data['First Name']+ ' ' + df_csv_data['Second Name']
df_json_data['Name']=df_json_data['firstName']+ ' ' + df_json_data['lastName']
df_xml_data['Name']=df_xml_data['firstname']+ ' ' + df_xml_data['lastname']

"Sort Data according to Name"
df_csv_data=df_csv_data.sort_values('Name').reset_index().drop('index', axis=1)
df_json_data=df_json_data.sort_values('Name').reset_index().drop('index', axis=1)
df_xml_data=df_xml_data.sort_values('Name').reset_index().drop('index', axis=1)

"Merge data"
Unified_Record=pd.concat([df_csv_data, df_json_data, df_xml_data ], axis=1)


"Drop duplicate columns"
Unified_Record = Unified_Record.loc[:,~Unified_Record.columns.duplicated()]
Unified_Record=Unified_Record.drop(['age', 'sex', 'firstName',	'lastName', 'lastname',	'firstname'], axis=1)
Unified_Record['dependants'].replace('', '0', inplace=True)
print ("Merged data \n", Unified_Record.head())

print('\n')

"""TEXT DATA"""
fob=open(Txt_data, 'r')
text=fob.read()
print("The text data reveals the following imformation: Debra Wood security code is 592, Howard Hilary works for Hussain-Adams and has a salary increase of 2000 \
Ms Molly Dobson clocked 82, Mr Miller pension is not 24515 but 27334")

print('\n')

"Data BEFORE making changes"
print("Data BEFORE making text changes: ")
print(Unified_Record.loc[Unified_Record['Name']=="Debra Wood", 'credit_card_security_code'])
print(Unified_Record.loc[Unified_Record['Name']=="Hilary Howard", 'salary'])
print(Unified_Record.loc[Unified_Record['Name']=="Molly Dobson", 'Age (Years)'])
print(Unified_Record.loc[Unified_Record['Name']=="Lewis Miller", 'pension'])

"Make the Changes revealed from the text data"
"Debra Wood"
Unified_Record.loc[Unified_Record['Name']=="Debra Wood", 'credit_card_security_code']= 592
"Howard Hilary"
Unified_Record.loc[Unified_Record['Name']=="Hilary Howard", 'salary']= int(Unified_Record.loc[Unified_Record['Name']=="Hilary Howard", 'salary'])+ 2000
"Ms Molly Dobson"
Unified_Record.loc[Unified_Record['Name']=="Molly Dobson", 'Age (Years)']=82
"Mr Miller"
Unified_Record.loc[(Unified_Record['Second Name']=="Miller") &  (Unified_Record['pension']=="24515"), 'pension']= '27334'

"Data AFTER making text changes"
print('\n \n')
print("Data AFTER making text changes: ")
print(Unified_Record.loc[Unified_Record['Name']=="Debra Wood", 'credit_card_security_code'])
print(Unified_Record.loc[Unified_Record['Name']=="Hilary Howard", 'salary'])
print(Unified_Record.loc[Unified_Record['Name']=="Molly Dobson", 'Age (Years)'])
print(Unified_Record.loc[Unified_Record['Name']=="Lewis Miller", 'pension'])



"""Writing data to MySQL DataBase"""

from pony.orm import *
db = Database()
db.bind(provider='mysql', host='europa.ashley.work', user='student_bh86hp', passwd='iE93F2@8EhM@1zhD&u9M@K', db='student_bh86hp')

class Records(db.Entity):
  FirstName=Required(str)
  SecondName=Required(str)
  address_postcode=Required(str)
  commute_distance=Required(str)
  company=Required(str)
  dependants=Required(str)
  marital_status=Required(str)	
  pension	=Required(str)
  retired	=Required(str)
  salary=Required(str)	
  Name= Required(str)	
  Age	= Required(int)
  Sex=Required(str)
  Vehicle_Make=Required(str)
  Vehicle_Model=Required(str)
  Vehicle_Year=Required(int)	
  Vehicle_Type= Required(str)
  iban=Required(str)
  credit_card_number=Required(str)	
  credit_card_security_code=Required(int)
  credit_card_start_date=Required(str)
  credit_card_end_date=Required(str)	
  address_main=Required(str)
  address_city=Required(str)	
  debt=Required(str)

db.generate_mapping(create_tables=True)

Unified_Records=Unified_Record.values.tolist()


with db_session:
    for record in Unified_Records:
      Records(FirstName=record[0],SecondName=record[1],Age=record[2],Sex=record[3],Vehicle_Make=record[4],Vehicle_Model=record[5],\
      Vehicle_Year=record[6],Vehicle_Type=record[7],Name=record[8],iban=record[9],credit_card_number= str(record[10]),credit_card_security_code	=record[11],\
      credit_card_start_date=record[12],credit_card_end_date=record[13],address_main=record[14],address_city=record[15],	address_postcode= record[16],debt=str(record[17]), \
      commute_distance=record[18],company=record[19],dependants=record[20],marital_status=record[21],	pension=record[22],retired=record[23],salary=str(record[24]) ) 
      commit()

print("SUCCESSFULLY COMMITED TO DATABASE")
