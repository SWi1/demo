---
layout: default
title: Extract and Merge NHANES cycles
parent: Generate Dietary Datasets - md
nav_order: 1
has_toc: true
---

# Step 1: Extract NHANES Cycles, Select Variabes of Interest, and combine into single dataframe


### Import packages

```python
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
import dask.dataframe as dd
import numpy as np
```

### Download NHANES data from web (cycles 2003 - 2023)


```python
print('Download & Read SAS Transport Files from web') 
# Demographic, 24-hr dietary recalls 1 and 2, and Food description data for cycles 03 through 18

demo_C = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2003/DataFiles/demo_c.xpt', format='xport', encoding='utf-8')
iff_1_C = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2003/DataFiles/dr1iff_c.xpt', format='xport', encoding='utf-8')
iff_2_C = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2003/DataFiles/dr2iff_c.xpt', format='xport', encoding='utf-8')
food_desc_C = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2003/DataFiles/drxfcd_c.xpt', format='xport', encoding='utf-8')

demo_D = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2005/DataFiles/demo_d.xpt', format='xport', encoding='utf-8')
iff_1_D = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2005/DataFiles/dr1iff_d.xpt', format='xport', encoding='utf-8')
iff_2_D = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2005/DataFiles/dr2iff_d.xpt', format='xport', encoding='utf-8')
food_desc_D = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2005/DataFiles/drxfcd_d.xpt', format='xport', encoding='utf-8')

demo_E = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2007/DataFiles/DEMO_E.xpt', format='xport', encoding='utf-8')
iff_1_E = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2007/DataFiles/DR1IFF_E.xpt', format='xport', encoding='utf-8')
iff_2_E = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2007/DataFiles/DR2IFF_E.xpt', format='xport', encoding='utf-8')
food_desc_E = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2007/DataFiles/drxfcd_e.xpt', format='xport', encoding='utf-8')

demo_F = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2009/DataFiles/DEMO_f.xpt', format='xport', encoding='utf-8')
iff_1_F = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2009/DataFiles/DR1IFF_f.xpt', format='xport', encoding='utf-8')
iff_2_F = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2009/DataFiles/DR2IFF_f.xpt', format='xport', encoding='utf-8')
food_desc_F = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2009/DataFiles/drxfcd_f.xpt', format='xport', encoding='utf-8')

demo_G = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2011/DataFiles/DEMO_g.xpt', format='xport', encoding='utf-8')
iff_1_G = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/nhanes/Public/2011/DataFiles/DR1IFF_G.XPT', format='xport', encoding='utf-8')
iff_2_G = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/nhanes/Public/2011/DataFiles/DR2IFF_g.xpt', format='xport', encoding='utf-8')
food_desc_G = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2011/DataFiles/drxfcd_g.xpt', format='xport', encoding='utf-8')

demo_H = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2013/DataFiles/DEMO_h.xpt', format='xport', encoding='utf-8')
iff_1_H = pd.read_sas('https://wwwn.cdc.gov/nchs/Data/nhanes/Public/2013/DataFiles/DR1IFF_H.XPT', format='xport', encoding='utf-8')
iff_2_H = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2013/DataFiles/DR2IFF_h.xpt', format='xport', encoding='utf-8')
food_desc_H = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2013/DataFiles/drxfcd_h.xpt', format='xport', encoding='latin-1') # error: 'utf-8' codec can't decode. using 'latin-1' works

demo_I = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2015/DataFiles/DEMO_I.xpt', format='xport', encoding='utf-8')
iff_1_I = pd.read_sas('https://wwwn.cdc.gov/nchs/Data/nhanes/Public/2015/DataFiles/DR1IFF_I.XPT', format='xport', encoding='utf-8')
iff_2_I = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2015/DataFiles/DR2IFF_I.xpt', format='xport', encoding='utf-8')
food_desc_I = pd.read_sas('https://wwwn.cdc.gov/NCHS/Data/nhanes/Public/2015/DataFiles/drxfcd_i.xpt', format='xport', encoding='utf-8')

demo_P = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2017/DataFiles/P_DEMO.xpt', format='xport', encoding='utf-8')
iff_1_P = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2017/DataFiles/P_DR1IFF.xpt', format='xport', encoding='utf-8')
iff_2_P = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2017/DataFiles/P_DR2IFF.xpt', format='xport', encoding='utf-8')
food_desc_P = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2017/DataFiles/P_DRXFCD.xpt', format='xport', encoding='utf-8')

demo_L = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/DEMO_L.xpt', format='xport', encoding='utf-8')
iff_1_L = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/DR1IFF_L.xpt', format='xport', encoding='utf-8')
iff_2_L = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/DR2IFF_L.xpt', format='xport', encoding='utf-8')
food_desc_L = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/DRXFCD_L.xpt', format='xport', encoding='utf-8')

print('Finished downloading NHANES files') 
```

    Download & Read SAS Transport Files from web
    Finished downloading NHANES files
### Select variables of interest from demographic and dietary data for combining cycles


```python
demo_C = demo_C[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'RIDEXPRG', 'DMDBORN', 'INDFMPIR', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']]
iff_1_C = iff_1_C[['SEQN', 'DRDINT', 'DR1DRSTZ', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'WTDR2D', 'DR1IGRMS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE', 'DR1IMOIS']].rename(columns={'DR1IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE', 'DR1IMOIS':'DR2IMOIS'}) # renaming to the day 2 feature names for averaging
iff_1_C['diet_day'] = 1
iff_2_C = iff_2_C[['SEQN', 'DRDINT', 'DR2DRSTZ', 'DR2ILINE', 'DR2IFDCD', 'DR2_030Z', 'WTDRD1', 'WTDR2D', 'DR2IGRMS', 'DR2IKCAL', 'DR2ICARB', 'DR2ISUGR', 'DR2IFIBE', 'DR2IMOIS']].rename(columns={'DR2IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1IFDCD':'DR2IFDCD', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_2_C['DR2ILINE'] = iff_2_C['DR2ILINE'] + 100 # combining two days of diet recalls, need unique DR Line items for ingredientization; add 100 to each line item for 2nd recalls for unique values
iff_2_C['diet_day'] = 2
iff_C = pd.concat([iff_1_C, iff_2_C])
iff_C['diet_wts'] = iff_C['WTDR2D']
iff_C = iff_C.drop(columns=['WTDRD1', 'WTDR2D'])
food_desc_C.drop(columns='DRXFCSD', inplace=True)
recall_1_C = pd.merge(food_desc_C, iff_C, on = 'DRXFDCD')
recall_1_C_ = pd.merge(recall_1_C, demo_C, on = 'SEQN')
recall_1_C_['CYCLE'] = '03_04'
recall_1_C_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_C_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_C_['DMDBORN'].replace({1 : "US", 2 : "Mexico", 3 : "Elsewhere", 7 : 'Unknown'}, inplace=True)

demo_D = demo_D[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'RIDEXPRG', 'DMDBORN', 'INDFMPIR', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']]
iff_1_D = iff_1_D[['SEQN', 'DRDINT', 'DR1DRSTZ', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'WTDR2D', 'DR1IGRMS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE', 'DR1IMOIS']].rename(columns={'DR1IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE', 'DR1IMOIS':'DR2IMOIS'})
iff_1_D['diet_day'] = 1
iff_2_D = iff_2_D[['SEQN', 'DRDINT', 'DR2DRSTZ', 'DR2ILINE', 'DR2IFDCD', 'DR2_030Z', 'WTDRD1', 'WTDR2D', 'DR2IGRMS', 'DR2IKCAL', 'DR2ICARB', 'DR2ISUGR', 'DR2IFIBE', 'DR2IMOIS']].rename(columns={'DR2IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1IFDCD':'DR2IFDCD', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_2_D['DR2ILINE'] = iff_2_D['DR2ILINE'] + 100 # combining two days of diet recalls, need unique DR Line items for ingredientization; add 100 to each line item for 2nd recalls for unique values
iff_2_D['diet_day'] = 2
iff_D = pd.concat([iff_1_D, iff_2_D])
iff_D['diet_wts'] = iff_D['WTDR2D']
iff_D = iff_D.drop(columns=['WTDRD1', 'WTDR2D'])
food_desc_D.drop(columns='DRXFCSD', inplace=True)
recall_1_D = pd.merge(food_desc_D, iff_D, on = 'DRXFDCD')
recall_1_D_ = pd.merge(recall_1_D, demo_D, on = 'SEQN')
recall_1_D_['CYCLE'] = '05_06'
recall_1_D_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_D_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_D_['DMDBORN'].replace({1 : "US", 2 : "Mexico", 3 : "Elsewhere", 7 : 'Unknown'}, inplace=True)

demo_E = demo_E[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'RIDEXPRG', 'DMDBORN2', 'INDFMPIR', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']].rename(columns={'DMDBORN2':'DMDBORN'})
iff_1_E = iff_1_E[['SEQN', 'DRDINT', 'DR1DRSTZ', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'WTDR2D', 'DR1IGRMS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE', 'DR1IMOIS']].rename(columns={'DR1IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE', 'DR1IMOIS':'DR2IMOIS'})
iff_1_E['diet_day'] = 1
iff_2_E = iff_2_E[['SEQN', 'DRDINT', 'DR2DRSTZ', 'DR2ILINE', 'DR2IFDCD', 'DR2_030Z', 'WTDRD1', 'WTDR2D', 'DR2IGRMS', 'DR2IKCAL', 'DR2ICARB', 'DR2ISUGR', 'DR2IFIBE', 'DR2IMOIS']].rename(columns={'DR2IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1IFDCD':'DR2IFDCD', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_2_E['DR2ILINE'] = iff_2_E['DR2ILINE'] + 100 # combining two days of diet recalls, need unique DR Line items for ingredientization; add 100 to each line item for 2nd recalls for unique values
iff_2_E['diet_day'] = 2
iff_E = pd.concat([iff_1_E, iff_2_E])
iff_E['diet_wts'] = iff_E['WTDR2D']
iff_E = iff_E.drop(columns=['WTDRD1', 'WTDR2D'])
food_desc_E.drop(columns='DRXFCSD', inplace=True)
recall_1_E = pd.merge(food_desc_E, iff_E, on = 'DRXFDCD')
recall_1_E_ = pd.merge(recall_1_E, demo_E, on = 'SEQN')
recall_1_E_['CYCLE'] = '07_08'
recall_1_E_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_E_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_E_['DMDBORN'].replace({1 : "US", 2 : "Mexico", 4 : "Other_Spanish_Speaking_Country", 5 : 'Other_Non-Spanish_Speaking_Country', 7 : 'Unknown', 9 : 'Unknown'}, inplace=True)

demo_F = demo_F[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'RIDEXPRG', 'DMDBORN2', 'INDFMPIR', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']].rename(columns={'DMDBORN2':'DMDBORN'})
iff_1_F = iff_1_F[['SEQN', 'DRDINT', 'DR1DRSTZ', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'WTDR2D', 'DR1IGRMS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE', 'DR1IMOIS']].rename(columns={'DR1IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE', 'DR1IMOIS':'DR2IMOIS'})
iff_1_F['diet_day'] = 1
iff_2_F = iff_2_F[['SEQN', 'DRDINT', 'DR2DRSTZ', 'DR2ILINE', 'DR2IFDCD', 'DR2_030Z', 'WTDRD1', 'WTDR2D', 'DR2IGRMS', 'DR2IKCAL', 'DR2ICARB', 'DR2ISUGR', 'DR2IFIBE', 'DR2IMOIS']].rename(columns={'DR2IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1IFDCD':'DR2IFDCD', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_2_F['DR2ILINE'] = iff_2_F['DR2ILINE'] + 100 # combining two days of diet recalls, need unique DR Line items for ingredientization; add 100 to each line item for 2nd recalls for unique values
iff_2_F['diet_day'] = 2
iff_F = pd.concat([iff_1_F, iff_2_F])
iff_F['diet_wts'] = iff_F['WTDR2D']
iff_F = iff_F.drop(columns=['WTDRD1', 'WTDR2D'])
food_desc_F.drop(columns='DRXFCSD', inplace=True)
recall_1_F = pd.merge(food_desc_F, iff_F, on = 'DRXFDCD')
recall_1_F_ = pd.merge(recall_1_F, demo_F, on = 'SEQN')
recall_1_F_['CYCLE'] = '09_10'
recall_1_F_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_F_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_F_['DMDBORN'].replace({1 : "US", 2 : "Mexico", 4 : "Other_Spanish_Speaking_Country", 5 : 'Other_Non-Spanish_Speaking_Country', 7 : 'Unknown', 9 : 'Unknown'}, inplace=True)

demo_G = demo_G[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'RIDEXPRG', 'DMDBORN4', 'INDFMPIR', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']].rename(columns={'DMDBORN4':'DMDBORN'})
iff_1_G = iff_1_G[['SEQN', 'DRDINT', 'DR1DRSTZ', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'WTDR2D', 'DR1IGRMS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE', 'DR1IMOIS']].rename(columns={'DR1IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE', 'DR1IMOIS':'DR2IMOIS'})
iff_1_G['diet_day'] = 1
iff_2_G = iff_2_G[['SEQN', 'DRDINT', 'DR2DRSTZ', 'DR2ILINE', 'DR2IFDCD', 'DR2_030Z', 'WTDRD1', 'WTDR2D', 'DR2IGRMS', 'DR2IKCAL', 'DR2ICARB', 'DR2ISUGR', 'DR2IFIBE', 'DR2IMOIS']].rename(columns={'DR2IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1IFDCD':'DR2IFDCD', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_2_G['DR2ILINE'] = iff_2_G['DR2ILINE'] + 100 # combining two days of diet recalls, need unique DR Line items for ingredientization; add 100 to each line item for 2nd recalls for unique values
iff_2_G['diet_day'] = 2
iff_G = pd.concat([iff_1_G, iff_2_G])
iff_G['diet_wts'] = iff_G['WTDR2D']
iff_G = iff_G.drop(columns=['WTDRD1', 'WTDR2D'])
food_desc_G.drop(columns='DRXFCSD', inplace=True)
recall_1_G = pd.merge(food_desc_G, iff_G, on = 'DRXFDCD')
recall_1_G_ = pd.merge(recall_1_G, demo_G, on = 'SEQN')
recall_1_G_['CYCLE'] = '11_12'
recall_1_G_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_G_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_G_['DMDBORN'].replace({1 : "US", 2 : "Elsewhere", 77 : 'Unknown', 99 : 'Unknown'}, inplace=True)

demo_H = demo_H[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'RIDEXPRG', 'DMDBORN4', 'INDFMPIR', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']].rename(columns={'DMDBORN4':'DMDBORN'})
iff_1_H = iff_1_H[['SEQN', 'DRDINT', 'DR1DRSTZ', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'WTDR2D', 'DR1IGRMS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE', 'DR1IMOIS']].rename(columns={'DR1IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE', 'DR1IMOIS':'DR2IMOIS'})
iff_1_H['diet_day'] = 1
iff_2_H = iff_2_H[['SEQN', 'DRDINT', 'DR2DRSTZ', 'DR2ILINE', 'DR2IFDCD', 'DR2_030Z', 'WTDRD1', 'WTDR2D', 'DR2IGRMS', 'DR2IKCAL', 'DR2ICARB', 'DR2ISUGR', 'DR2IFIBE', 'DR2IMOIS']].rename(columns={'DR2IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1IFDCD':'DR2IFDCD', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_2_H['DR2ILINE'] = iff_2_H['DR2ILINE'] + 100 # combining two days of diet recalls, need unique DR Line items for ingredientization; add 100 to each line item for 2nd recalls for unique values
iff_2_H['diet_day'] = 2
iff_H = pd.concat([iff_1_H, iff_2_H])
iff_H['diet_wts'] = iff_H['WTDR2D']
iff_H = iff_H.drop(columns=['WTDRD1', 'WTDR2D'])
food_desc_H.drop(columns='DRXFCSD', inplace=True)
recall_1_H = pd.merge(food_desc_H, iff_H, on = 'DRXFDCD')
recall_1_H_ = pd.merge(recall_1_H, demo_H, on = 'SEQN')
recall_1_H_['CYCLE'] = '13_14'
recall_1_H_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_H_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_H_['DMDBORN'].replace({1 : "US", 2 : "Elsewhere", 77 : 'Unknown', 99 : 'Unknown'}, inplace=True)

demo_I = demo_I[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'RIDEXPRG', 'DMDBORN4', 'INDFMPIR', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']].rename(columns={'DMDBORN4':'DMDBORN'})
iff_1_I = iff_1_I[['SEQN', 'DRDINT', 'DR1DRSTZ', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'WTDR2D', 'DR1IGRMS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE', 'DR1IMOIS']].rename(columns={'DR1IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE', 'DR1IMOIS':'DR2IMOIS'})
iff_1_I['diet_day'] = 1
iff_2_I = iff_2_I[['SEQN', 'DRDINT', 'DR2DRSTZ', 'DR2ILINE', 'DR2IFDCD', 'DR2_030Z', 'WTDRD1', 'WTDR2D', 'DR2IGRMS', 'DR2IKCAL', 'DR2ICARB', 'DR2ISUGR', 'DR2IFIBE', 'DR2IMOIS']].rename(columns={'DR2IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1IFDCD':'DR2IFDCD', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_2_I['DR2ILINE'] = iff_2_I['DR2ILINE'] + 100 # combining two days of diet recalls, need unique DR Line items for ingredientization; add 100 to each line item for 2nd recalls for unique values
iff_2_I['diet_day'] = 2
iff_I = pd.concat([iff_1_I, iff_2_I])
iff_I['diet_wts'] = iff_I['WTDR2D']
iff_I = iff_I.drop(columns=['WTDRD1', 'WTDR2D'])
food_desc_I.drop(columns='DRXFCSD', inplace=True)
recall_1_I = pd.merge(food_desc_I, iff_I, on = 'DRXFDCD')
recall_1_I_ = pd.merge(recall_1_I, demo_I, on = 'SEQN')
recall_1_I_['CYCLE'] = '15_16'
recall_1_I_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_I_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_I_['DMDBORN'].replace({1 : "US", 2 : "Elsewhere", 77 : 'Unknown', 99 : 'Unknown'}, inplace=True)

demo_P = demo_P[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'RIDEXPRG', 'DMDBORN4', 'INDFMPIR', 'DMDYRUSZ', 'DMDEDUC2', 'WTINTPRP', 'WTMECPRP', 'SDMVPSU', 'SDMVSTRA']].rename(columns={'DMDBORN4':'DMDBORN', 'DMDYRUSZ': 'DMDYRSUS'})
demo_P['DMDEDUC3'] = float('nan') # no education data collected for persons under 20 y/o. Generating NAs for merging with earlier cycles
iff_1_P = iff_1_P[['SEQN', 'DRDINT', 'DR1DRSTZ', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1PP', 'WTDR2DPP', 'DR1IGRMS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE', 'DR1IMOIS']].rename(columns={'DR1IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1_030Z':'DR2_030Z', 'WTDRD1PP':'WTDRD1', 'WTDR2DPP':'WTDR2D', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE', 'DR1IMOIS':'DR2IMOIS'})
iff_1_P['diet_day'] = 1
iff_2_P = iff_2_P[['SEQN', 'DRDINT', 'DR2DRSTZ', 'DR2ILINE', 'DR2IFDCD', 'DR2_030Z', 'WTDRD1PP', 'WTDR2DPP', 'DR2IGRMS', 'DR2IKCAL', 'DR2ICARB', 'DR2ISUGR', 'DR2IFIBE', 'DR2IMOIS']].rename(columns={'DR2IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1IFDCD':'DR2IFDCD', 'DR1_030Z':'DR2_030Z', 'WTDRD1PP':'WTDRD1', 'WTDR2DPP':'WTDR2D', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_2_P['DR2ILINE'] = iff_2_P['DR2ILINE'] + 100 # combining two days of diet recalls, need unique DR Line items for ingredientization; add 100 to each line item for 2nd recalls for unique values
iff_2_P['diet_day'] = 2
iff_P = pd.concat([iff_1_P, iff_2_P])
iff_P['diet_wts'] = iff_P['WTDR2D']
iff_P = iff_P.drop(columns=['WTDRD1', 'WTDR2D'])
food_desc_P.drop(columns='DRXFCSD', inplace=True)
recall_1_P = pd.merge(food_desc_P, iff_P, on = 'DRXFDCD')
recall_1_P_ = pd.merge(recall_1_P, demo_P, on = 'SEQN')
recall_1_P_['CYCLE'] = '17_20'
recall_1_P_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_P_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_P_['DMDBORN'].replace({1 : "US", 2 : "Elsewhere", 77 : 'Unknown', 99 : 'Unknown'}, inplace=True)

demo_L = demo_L[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'RIDEXPRG', 'DMDBORN4', 'INDFMPIR', 'DMDYRUSR', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']].rename(columns={'DMDBORN4':'DMDBORN', 'DMDYRUSR': 'DMDYRSUS'})
demo_L['DMDEDUC3'] = float('nan') # no education data collected for persons under 20 y/o. Generating NAs for merging with earlier cycles
iff_1_L = iff_1_L[['SEQN', 'DRDINT', 'DR1DRSTZ', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'WTDR2D', 'DR1IGRMS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE', 'DR1IMOIS']].rename(columns={'DR1IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE', 'DR1IMOIS':'DR2IMOIS'})
iff_1_L['diet_day'] = 1
iff_2_L = iff_2_L[['SEQN', 'DRDINT', 'DR2DRSTZ', 'DR2ILINE', 'DR2IFDCD', 'DR2_030Z', 'WTDRD1', 'WTDR2D', 'DR2IGRMS', 'DR2IKCAL', 'DR2ICARB', 'DR2ISUGR', 'DR2IFIBE', 'DR2IMOIS']].rename(columns={'DR2IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1IFDCD':'DR2IFDCD', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_2_L['DR2ILINE'] = iff_2_L['DR2ILINE'] + 100 # combining two days of diet recalls, need unique DR Line items for ingredientization; add 100 to each line item for 2nd recalls for unique values
iff_2_L['diet_day'] = 2
iff_L = pd.concat([iff_1_L, iff_2_L])
iff_L['diet_wts'] = iff_L['WTDR2D']
iff_L = iff_L.drop(columns=['WTDRD1', 'WTDR2D'])
food_desc_L.drop(columns='DRXFCSD', inplace=True)
recall_1_L = pd.merge(food_desc_L, iff_L, on = 'DRXFDCD')
recall_1_L_ = pd.merge(recall_1_L, demo_L, on = 'SEQN')
recall_1_L_['CYCLE'] = '21_22'
recall_1_L_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_L_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_L_['DMDBORN'].replace({1 : "US", 2 : "Elsewhere", 77 : 'Unknown', 99 : 'Unknown'}, inplace=True)
```

### Combine all cycles into a single dataframe and exclude participants without two 24-hr recalls, or diet data not passing quality control


```python
WWEIA_ALL = pd.concat([recall_1_C_, recall_1_D_, recall_1_E_, recall_1_F_, recall_1_G_, recall_1_H_, recall_1_I_, recall_1_P_, recall_1_L_])

# remove individuals with only one 24-hr recall
WWEIA_ALL = WWEIA_ALL[WWEIA_ALL['DRDINT']==2]

# remove individuals with diet data not passing quality control
WWEIA_ALL = WWEIA_ALL[WWEIA_ALL['DR2DRSTZ']==1]
```
