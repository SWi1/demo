---
layout: default
title: Generate Dietary Datasets - md
nav_order: 3
show_toc: true
---

### Title: 00_generate_datasets
### Purpose: Generate four WWEIA datasets: mixed meals, food categories, total nutrients, ingredients
### Date: July 24, 2024
### Author: Jules Larke

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

### Cross walk food codes and descriptions to harmonize data across cycles

#### NHANES uses the Food and Nutrient Database for Dietary Studies (FNDDS) to generate nutrient intakes from food composition data. FNDDS is released every two-years in conjunction with the WWEIA, NHANES dietary data release. For each new version of FNDDS, foods/beverages, portions, and nutrient values are reviewed and updated. As we are combining many NHANES/WWEIA cycles, we are going to use the cross-walk to provide the most updated versions of FNDDS foods and beverages to harmonize the changes over the cycle years


```python
# read in the FNDDS cross-walks to harmonize foodcodes
# apply, combine for each cycle to correctly crosswalk codes
xwalk_FG = pd.read_csv('../../data/00/fndds_crosswalk/fndds_0910_1112_crosswalk.csv')
xwalk_GH = pd.read_csv('../../data/00/fndds_crosswalk/fndds_1112_1314_crosswalk.csv')
xwalk_HI = pd.read_csv('../../data/00/fndds_crosswalk/fndds_1314_1516_crosswalk.csv')
xwalk_IJ = pd.read_csv('../../data/00/fndds_crosswalk/fndds_1516_1718_crosswalk.csv')
xwalk_JP = pd.read_csv('../../data/00/fndds_crosswalk/fndds_1718_1920_crosswalk.csv')
xwalk_PL = pd.read_csv('../../data/00/fndds_crosswalk/fndds_1920_2123_crosswalk.csv')

# combine crosswalk cycles
xwalk_all = pd.concat([xwalk_PL, xwalk_JP, xwalk_IJ, xwalk_HI, xwalk_GH, xwalk_FG])

# drop_duplicates (takes most recent change)
xwalk_all.drop_duplicates(subset='DRXFDCD', inplace=True)

# change to int for merging
xwalk_all['foodcode'] = xwalk_all['foodcode'].astype(int)
```


```python
# Determine which FoodCodes have changed across cycles
merged_with_cycles = WWEIA_ALL.merge(
    xwalk_all[['DRXFDCD', 'foodcode', 'food_description', 'CYCLE']], 
    on='DRXFDCD', 
    how='left',
    suffixes=('_original', '_update')  # This will create CYCLE_original and CYCLE_update
)

# For records that were updated, use the update cycle; for others, use original cycle
merged_with_cycles['effective_cycle'] = merged_with_cycles['CYCLE_update'].fillna(merged_with_cycles['CYCLE_original'])

# Update the food codes and descriptions where matches exist
merged_with_cycles['DRXFDCD_final'] = merged_with_cycles['foodcode'].fillna(merged_with_cycles['DRXFDCD'])
merged_with_cycles['DRXFCLD_final'] = merged_with_cycles['food_description'].fillna(merged_with_cycles['DRXFCLD'])

# Get the most recent description for each food code
most_recent_descriptions = (merged_with_cycles
                           .sort_values('effective_cycle', ascending=False)
                           .groupby('DRXFDCD_final')
                           .agg({
                               'DRXFCLD_final': 'first',  # Most recent description
                               'effective_cycle': 'first'  # The cycle it came from
                           })
                           .reset_index())

# Create your final harmonized dataset
harmonized_codes = most_recent_descriptions.rename(columns={
    'DRXFDCD_final': 'DRXFDCD',
    'DRXFCLD_final': 'DRXFCLD'
})[['DRXFDCD', 'DRXFCLD']]

# Check the results
print(f"Original: {WWEIA_ALL['DRXFDCD'].nunique()} codes, {WWEIA_ALL['DRXFCLD'].nunique()} descriptions")
print(f"Harmonized: {harmonized_codes['DRXFDCD'].nunique()} codes, {harmonized_codes['DRXFCLD'].nunique()} descriptions")

# Show which cycles contributed
cycle_contributions = most_recent_descriptions['effective_cycle'].value_counts().sort_index()
print(f"\nDescriptions by cycle:")
print(cycle_contributions)
```

    Original: 9635 codes, 13327 descriptions
    Harmonized: 6896 codes, 6896 descriptions
    
    Descriptions by cycle:
    effective_cycle
    03_04     152
    05_06      87
    07_08     117
    09_10     233
    11_12     257
    13_14     463
    15_16     413
    17_20     837
    21_22    4337
    Name: count, dtype: int64



```python
# Apply the harmonized food codes and descriptions to the full dataset
WWEIA_ALL_harmonized = WWEIA_ALL.merge(
    harmonized_codes[['DRXFDCD', 'DRXFCLD']], 
    on='DRXFDCD', 
    how='left',
    suffixes=('_old', '_harmonized')
)

# Replace the old food descriptions with harmonized ones
WWEIA_ALL_harmonized['DRXFCLD'] = WWEIA_ALL_harmonized['DRXFCLD_harmonized']

# Drop the temporary columns
WWEIA_ALL_harmonized = WWEIA_ALL_harmonized.drop(['DRXFCLD_old', 'DRXFCLD_harmonized'], axis=1)

# Check the results
print(f"Original WWEIA_ALL shape: {WWEIA_ALL.shape}")
print(f"Harmonized WWEIA_ALL shape: {WWEIA_ALL_harmonized.shape}")
print(f"Original unique descriptions: {WWEIA_ALL['DRXFCLD'].nunique()}")
print(f"Harmonized unique descriptions: {WWEIA_ALL_harmonized['DRXFCLD'].nunique()}")

# Verify no data was lost
print(f"Records in original: {len(WWEIA_ALL)}")
print(f"Records in harmonized: {len(WWEIA_ALL_harmonized)}")

# Check for any unmatched food codes
unmatched = WWEIA_ALL_harmonized['DRXFCLD'].isna().sum()
if unmatched > 0:
    print(f"Warning: {unmatched} records have missing descriptions after harmonization")
    # Show which codes are problematic
    problem_codes = WWEIA_ALL_harmonized[WWEIA_ALL_harmonized['DRXFCLD'].isna()]['DRXFDCD'].unique()
    print(f"Problematic codes: {problem_codes[:10]}...")  # Show first 10
```

    Original WWEIA_ALL shape: (2124046, 33)
    Harmonized WWEIA_ALL shape: (2124046, 33)
    Original unique descriptions: 13327
    Harmonized unique descriptions: 6871
    Records in original: 2124046
    Records in harmonized: 2124046
    Warning: 236505 records have missing descriptions after harmonization
    Problematic codes: [41601080. 57807010. 67104040. 76407000. 94000000. 11421000. 31103000.
     71802010. 92570100. 32104950.]...



```python
# Find the missing codes
missing_codes = WWEIA_ALL_harmonized[WWEIA_ALL_harmonized['DRXFCLD'].isna()]['DRXFDCD'].unique()
print(f"Number of missing food codes: {len(missing_codes)}")

# Check if these codes exist in your original harmonized_df
codes_in_harmonized = set(harmonized_codes['DRXFDCD'])
codes_in_original = set(WWEIA_ALL['DRXFDCD'])
missing_from_harmonization = codes_in_original - codes_in_harmonized

print(f"Codes in original WWEIA_ALL: {len(codes_in_original)}")
print(f"Codes in harmonized lookup: {len(codes_in_harmonized)}")
print(f"Codes missing from harmonization: {len(missing_from_harmonization)}")

# See some examples of missing codes and their original descriptions
missing_examples = WWEIA_ALL[WWEIA_ALL['DRXFDCD'].isin(list(missing_codes)[:10])][['DRXFDCD', 'DRXFCLD', 'CYCLE']].drop_duplicates()
print("\nExamples of missing codes:")
print(missing_examples)
```

    Number of missing food codes: 2764
    Codes in original WWEIA_ALL: 9635
    Codes in harmonized lookup: 6896
    Codes missing from harmonization: 2764
    
    Examples of missing codes:
               DRXFDCD                                            DRXFCLD  CYCLE
    1511    41601080.0                                    Pinto bean soup  03_04
    1520    57807010.0  Whole wheat cereal with apples, baby food, dry...  03_04
    1527    67104040.0       Applesauce with bananas, baby food, strained  03_04
    1544    76407000.0  Mixed vegetables, garden vegetables, baby food...  03_04
    1549    94000000.0                             Water as an ingredient  03_04
    1767    11421000.0  Yogurt, vanilla, lemon, or coffee flavor, whol...  03_04
    1774    31103000.0                                 Egg, whole, boiled  03_04
    1790    71802010.0                           Macaroni and potato soup  03_04
    3566    92570100.0            Fluid replacement, electrolyte solution  03_04
    3574    32104950.0  Egg omelet or scrambled egg, fat not added in ...  03_04
    737     94000000.0                             Water as an ingredient  05_06
    1371    92570100.0            Fluid replacement, electrolyte solution  05_06
    5024    31103000.0                                 Egg, whole, boiled  05_06
    5152    32104950.0  Egg omelet or scrambled egg, fat not added in ...  05_06
    8446    71802010.0                           Macaroni and potato soup  05_06
    15578   11421000.0  Yogurt, vanilla, lemon, or coffee flavor, whol...  05_06
    29824   41601080.0                                    Pinto bean soup  05_06
    64222   57807010.0  Whole wheat cereal with apples, baby food, dry...  05_06
    72513   67104040.0       Applesauce with bananas, baby food, strained  05_06
    1840    94000000.0                             Water as an ingredient  07_08
    3384    31103000.0                                 Egg, whole, boiled  07_08
    4919    32104950.0  Egg omelet or scrambled egg, fat not added in ...  07_08
    13214   11421000.0  Yogurt, vanilla, lemon, or coffee flavor, whol...  07_08
    16016   92570100.0            Fluid replacement, electrolyte solution  07_08
    99014   41601080.0                                    Pinto bean soup  07_08
    196865  67104040.0       Applesauce with bananas, baby food, strained  07_08
    197310  76407000.0  Mixed vegetables, garden vegetables, baby food...  07_08
    825     92570100.0            Fluid replacement, electrolyte solution  09_10
    827     94000000.0                             Water as an ingredient  09_10
    3560    31103000.0                                 Egg, whole, boiled  09_10
    7703    32104950.0  Egg omelet or scrambled egg, fat not added in ...  09_10
    11339   11421000.0  Yogurt, vanilla, lemon, or coffee flavor, whol...  09_10
    60156   67104040.0       Applesauce with bananas, baby food, strained  09_10
    212843  57807010.0  Whole wheat cereal with apples, baby food, dry...  09_10
    213915  76407000.0  Mixed vegetables, garden vegetables, baby food...  09_10
    222205  41601080.0                                    Pinto bean soup  09_10
    4792    11421000.0  Yogurt, vanilla, lemon, or coffee flavor, whol...  11_12
    163582  57807010.0  Whole wheat cereal with apples, baby food, dry...  11_12
    173746  67104040.0       Applesauce with bananas, baby food, strained  11_12
    173793  76407000.0  Mixed vegetables, garden vegetables, baby food...  11_12
    2419    11421000.0                        Yogurt, vanilla, whole milk  13_14
    174057  67104040.0       Applesauce with bananas, baby food, strained  13_14
    174683  76407000.0  Mixed vegetables, garden vegetables, baby food...  13_14
    175533  57807010.0  Whole wheat cereal with apples, baby food, dry...  13_14
    49040   67104040.0       Applesauce with bananas, baby food, strained  15_16
    125216  57807010.0  Whole wheat cereal with apples, baby food, dry...  15_16
    10895   41601080.0  Pinto bean soup, home recipe, canned or ready-...  17_20
    19204   67104040.0       Applesauce with bananas, baby food, strained  17_20
    214776  76407000.0  Mixed vegetables, garden vegetables, baby food...  17_20
    215807  57807010.0  Whole wheat cereal with apples, baby food, dry...  17_20



```python
# Create harmonized entries for the missing codes using their most recent descriptions
missing_codes_df = WWEIA_ALL[WWEIA_ALL['DRXFDCD'].isin(missing_from_harmonization)]

# For missing codes, get the most recent description from the original data
missing_harmonized = (missing_codes_df
                     .sort_values('CYCLE', ascending=False)
                     .groupby('DRXFDCD')
                     .agg({'DRXFCLD': 'first'})
                     .reset_index())

print(f"Missing codes to add: {len(missing_harmonized)}")

# Combine with your existing harmonized data
complete_harmonized_df = pd.concat([
    harmonized_codes,
    missing_harmonized
], ignore_index=True)

print(f"Complete harmonized lookup: {len(complete_harmonized_df)} codes")
print(f"Should equal original unique codes: {WWEIA_ALL['DRXFDCD'].nunique()}")

# Verify we have all codes now
missing_check = set(WWEIA_ALL['DRXFDCD']) - set(complete_harmonized_df['DRXFDCD'])
print(f"Still missing after adding: {len(missing_check)} codes")
```

    Missing codes to add: 2764
    Complete harmonized lookup: 9660 codes
    Should equal original unique codes: 9635
    Still missing after adding: 0 codes



```python
# Now apply the complete harmonization to your full dataset
WWEIA_ALL_harmonized_final = WWEIA_ALL.merge(
    complete_harmonized_df[['DRXFDCD', 'DRXFCLD']], 
    on='DRXFDCD', 
    how='left',
    suffixes=('_old', '_harmonized')
)

# Replace descriptions
WWEIA_ALL_harmonized_final['DRXFCLD'] = WWEIA_ALL_harmonized_final['DRXFCLD_harmonized']
WWEIA_ALL_harmonized_final = WWEIA_ALL_harmonized_final.drop(['DRXFCLD_old', 'DRXFCLD_harmonized'], axis=1)

# Final check
print(f"Final harmonized dataset:")
print(f"Shape: {WWEIA_ALL_harmonized_final.shape}")
print(f"Original unique descriptions: {WWEIA_ALL['DRXFCLD'].nunique()}")
print(f"Final harmonized unique descriptions: {WWEIA_ALL_harmonized_final['DRXFCLD'].nunique()}")
print(f"Missing descriptions: {WWEIA_ALL_harmonized_final['DRXFCLD'].isna().sum()}")

# Show the reduction in unique descriptions
reduction = WWEIA_ALL['DRXFCLD'].nunique() - WWEIA_ALL_harmonized_final['DRXFCLD'].nunique()
reduction_pct = (reduction / WWEIA_ALL['DRXFCLD'].nunique()) * 100
print(f"Reduced descriptions by: {reduction} ({reduction_pct:.1f}%)")
```

    Final harmonized dataset:
    Shape: (2124046, 33)
    Original unique descriptions: 13327
    Final harmonized unique descriptions: 9613
    Missing descriptions: 0
    Reduced descriptions by: 3714 (27.9%)



```python
# For each description, find the most recent food code based on cycle
most_recent_codes = (WWEIA_ALL_harmonized_final
                    .sort_values('CYCLE', ascending=False)
                    .groupby('DRXFCLD')
                    .agg({
                        'DRXFDCD': 'first',  # Most recent food code
                        'CYCLE': 'first'     # The cycle it came from
                    })
                    .reset_index())

print(f"Unique descriptions: {len(most_recent_codes)}")
print(f"This should equal unique codes after update")

# Create the final code-to-description mapping
final_harmonized_lookup = most_recent_codes[['DRXFDCD', 'DRXFCLD']].rename(columns={'DRXFDCD': 'DRXFDCD_updated'})

# Apply the updated food codes to your dataset
WWEIA_ALL_final = WWEIA_ALL_harmonized_final.merge(
    most_recent_codes[['DRXFCLD', 'DRXFDCD']].rename(columns={'DRXFDCD': 'DRXFDCD_updated'}),
    on='DRXFCLD',
    how='left'
)

# Replace the old food codes with the updated ones
WWEIA_ALL_final['DRXFDCD'] = WWEIA_ALL_final['DRXFDCD_updated']
WWEIA_ALL_final = WWEIA_ALL_final.drop('DRXFDCD_updated', axis=1)

# Check the results
print(f"\nFinal results:")
print(f"Shape: {WWEIA_ALL_final.shape}")
print(f"Unique food codes: {WWEIA_ALL_final['DRXFDCD'].nunique()}")
print(f"Unique descriptions: {WWEIA_ALL_final['DRXFCLD'].nunique()}")
print(f"Perfect 1:1 mapping: {WWEIA_ALL_final['DRXFDCD'].nunique() == WWEIA_ALL_final['DRXFCLD'].nunique()}")
```

    Unique descriptions: 9613
    This should equal unique codes after update
    
    Final results:
    Shape: (2124046, 33)
    Unique food codes: 9613
    Unique descriptions: 9613
    Perfect 1:1 mapping: True



```python
# Show which cycles contributed the most recent codes
cycle_contributions = most_recent_codes['CYCLE'].value_counts().sort_index()
print(f"\nMost recent codes by cycle:")
print(cycle_contributions)
```

    
    Most recent codes by cycle:
    CYCLE
    03_04     193
    05_06     142
    07_08     208
    09_10     669
    11_12     445
    13_14     921
    15_16    1081
    17_20    1619
    21_22    4335
    Name: count, dtype: int64



```python
# Save the uniques list of food codes and descriptions for generating in the food tree later on
final_harmonized_lookup = final_harmonized_lookup.rename(columns={'DRXFDCD_updated':'FoodCode', 'DRXFCLD': 'Main.food.description'})
final_harmonized_lookup['FoodCode'] = final_harmonized_lookup['FoodCode'].astype(int)
final_harmonized_lookup['FoodID'] = final_harmonized_lookup['FoodCode']
final_harmonized_lookup.to_csv('../../data/00/food_tree.txt', sep='\t', index=None)
```

### Generate the Mixed Meal (foodcode level) dataset


```python
# select columns
wweia_foodcode = WWEIA_ALL_final[['SEQN', 'DRXFDCD', 'DRXFCLD', 'DR2IGRMS', 'DR2IKCAL', 'DR2IMOIS',
       'diet_day', 'diet_wts', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'RIDEXPRG',
       'DMDBORN', 'INDFMPIR', 'DMDEDUC2', 'CYCLE']]

# make to copy to modify
wweia_foodcode = wweia_foodcode.copy()

# rename columns so they are readable
wweia_foodcode.rename(columns={'DRXFDCD': 'foodcode', 'DRXFCLD': 'food_description', 'DR2IGRMS': 'foodcode_intake_g', 'DR2IKCAL': 'foodcode_kcal'}, inplace=True)
```


```python
# average intake over 2 diet recall days (mixed meals)
# sum intakes
foodcode_sum = wweia_foodcode.groupby(['SEQN', 'diet_day', 'foodcode', 'food_description'])[['foodcode_intake_g', 'foodcode_kcal']].agg(np.sum).reset_index()
foodcode_sum.set_index(['SEQN', 'diet_day', 'foodcode', 'food_description'],inplace=True)
r_sum = foodcode_sum.unstack(level=['diet_day'], fill_value=0).stack()
r_sum.reset_index(inplace=True)

# average intakes
foodcode_mean = r_sum.groupby(['SEQN', 'foodcode', 'food_description'])[['foodcode_intake_g', 'foodcode_kcal']].mean().reset_index()

# Save dataset
foodcode_mean.to_csv('../../data/00/wweia_dataset/wweia_foodcode_recalls_2023.csv', index=None)

# will add metadata from the ingredients dataset in the next script (01_build_wweia_2023.ipynb)
```

### Generate FPED dataset


```python
# convert foodcode to integer for safe merging
foodcode_mean['foodcode'] = foodcode_mean['foodcode'].astype(int)

# read in the combimed FPED cycle data
fped = pd.read_csv('../../data/00/wweia_fped/wweia_fped_all.csv')

# rename column for merging
fped = fped.rename(columns={'FOODCODE':'foodcode'})

# merge FPED with mixed meal data
fped_code = foodcode_mean.merge(fped, on='foodcode',how='left')


```


```python
# Check missing values after merge
fped_code[fped_code['DESCRIPTION'].isna()] # 116 unique foodcodes corresponding to 4652 rows missing in FPED data

# calculate the percent of data missing (no FPED data)
print(round(fped_code[fped_code['DESCRIPTION'].isna()].shape[0]/(fped_code.shape[0]-fped_code[fped_code['DESCRIPTION'].isna()].shape[0]),3)*100, 'percent of total rows without FPED')
```

    0.3 percent of total rows without FPED



```python
# drop NAs (the 4652 rows missing FPED data)
fped_code = fped_code.dropna()

# drop cycle column, no longer needed
fped_code = fped_code.drop(columns='cycle')

# save the FPED dataset
fped_code.to_csv('../../data/00/wweia_dataset/wweia_fped_recalls_2023.csv', index=None)
```

### Generate WWEIA total nutrients dataset


```python
# Download Total Nutritent Intake Data for Day 1 and 2:
import pandas as pd

nut_1_C = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2003/DataFiles/DR1TOT_C.xpt', format='xport', encoding='utf-8')
nut_2_C = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2003/DataFiles/DR2TOT_C.xpt', format='xport', encoding='utf-8')

nut_1_D = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2005/DataFiles/DR1TOT_D.xpt', format='xport', encoding='utf-8')
nut_2_D = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2005/DataFiles/DR2TOT_D.xpt', format='xport', encoding='utf-8')

nut_1_E = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2007/DataFiles/DR1TOT_E.xpt', format='xport', encoding='utf-8')
nut_2_E = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2007/DataFiles/DR2TOT_E.xpt', format='xport', encoding='utf-8')

nut_1_F = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2009/DataFiles/DR1TOT_F.xpt', format='xport', encoding='utf-8')
nut_2_F = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2009/DataFiles/DR2TOT_F.xpt', format='xport', encoding='utf-8')

nut_1_G = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2011/DataFiles/DR1TOT_G.xpt', format='xport', encoding='utf-8')
nut_2_G = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2011/DataFiles/DR2TOT_G.xpt', format='xport', encoding='utf-8')

nut_1_H = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2013/DataFiles/DR1TOT_H.xpt', format='xport', encoding='utf-8')
nut_2_H = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2013/DataFiles/DR2TOT_H.xpt', format='xport', encoding='utf-8')

nut_1_I = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2015/DataFiles/DR1TOT_I.xpt', format='xport', encoding='utf-8')
nut_2_I = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2015/DataFiles/DR2TOT_I.xpt', format='xport', encoding='utf-8')

nut_1_P = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2017/DataFiles/P_DR1TOT.xpt', format='xport', encoding='utf-8')
nut_2_P = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2017/DataFiles/P_DR2TOT.xpt', format='xport', encoding='utf-8')

nut_1_L = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/DR1TOT_L.xpt', format='xport', encoding='utf-8')
nut_2_L = pd.read_sas('https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/DR2TOT_L.xpt', format='xport', encoding='utf-8')
```


```python
# clean datasets by keeping only particpants with 2 recall days 'DRDINT' == 2 and passing quality control 'DR1DRSTZ' == 1
# each cycle has two days of nutrient intakes (corresponding to the 24-hr recall), e.g., nut_1_C and nut_2_C

nut_1_C = nut_1_C[nut_1_C['DRDINT']==2]
nut_1_C = nut_1_C[nut_1_C['DR1DRSTZ']==1]

nut_2_C = nut_2_C[nut_2_C['DRDINT']==2]
nut_2_C = nut_2_C[nut_2_C['DR2DRSTZ']==1]

nut_1_D = nut_1_D[nut_1_D['DRDINT']==2]
nut_1_D = nut_1_D[nut_1_D['DR1DRSTZ']==1]

nut_2_D = nut_2_D[nut_2_D['DRDINT']==2]
nut_2_D = nut_2_D[nut_2_D['DR2DRSTZ']==1]

nut_1_F = nut_1_F[nut_1_F['DRDINT']==2]
nut_1_F = nut_1_F[nut_1_F['DR1DRSTZ']==1]

nut_2_F = nut_2_F[nut_2_F['DRDINT']==2]
nut_2_F = nut_2_F[nut_2_F['DR2DRSTZ']==1]

nut_1_G = nut_1_G[nut_1_G['DRDINT']==2]
nut_1_G = nut_1_G[nut_1_G['DR1DRSTZ']==1]

nut_2_G = nut_2_G[nut_2_G['DRDINT']==2]
nut_2_G = nut_2_G[nut_2_G['DR2DRSTZ']==1]

nut_1_H = nut_1_H[nut_1_H['DRDINT']==2]
nut_1_H = nut_1_H[nut_1_H['DR1DRSTZ']==1]

nut_2_H = nut_2_H[nut_2_H['DRDINT']==2]
nut_2_H = nut_2_H[nut_2_H['DR2DRSTZ']==1]

nut_1_I = nut_1_I[nut_1_I['DRDINT']==2]
nut_1_I = nut_1_I[nut_1_I['DR1DRSTZ']==1]

nut_2_I = nut_2_I[nut_2_I['DRDINT']==2]
nut_2_I = nut_2_I[nut_2_I['DR2DRSTZ']==1]

nut_1_P = nut_1_P[nut_1_P['DRDINT']==2]
nut_1_P = nut_1_P[nut_1_P['DR1DRSTZ']==1]

nut_2_P = nut_2_P[nut_2_P['DRDINT']==2]
nut_2_P = nut_2_P[nut_2_P['DR2DRSTZ']==1]

nut_1_L = nut_1_L[nut_1_L['DRDINT']==2]
nut_1_L = nut_1_L[nut_1_L['DR1DRSTZ']==1]

nut_2_L = nut_2_L[nut_2_L['DRDINT']==2]
nut_2_L = nut_2_L[nut_2_L['DR2DRSTZ']==1]
```


```python
# For each cycle get participant IDs common for day 1 and day 2 recalls

# Define the cycles for your datasets
cycle = ['C', 'D', 'E', 'F', 'G', 'H', 'I', 'P', 'L']

# Store your datasets in dictionaries for easy access
nut_1_datasets = {}
nut_2_datasets = {}

for letter in cycle:
    
    nut_1_datasets[letter] = globals()[f'nut_1_{letter}']
    nut_2_datasets[letter] = globals()[f'nut_2_{letter}']

# Filter each pair of datasets to include only matching SEQN values
nut_1_filtered = {}
nut_2_filtered = {}

print("Processing each cycle pair:")
print("-" * 50)

for letter in cycle:
    # Find common SEQN values between the pair
    common_seqn_pair = set(nut_1_datasets[letter]['SEQN']).intersection(
        set(nut_2_datasets[letter]['SEQN'])
    )
    
    # Filter each dataset in the pair
    nut_1_filtered[letter] = nut_1_datasets[letter][
        nut_1_datasets[letter]['SEQN'].isin(common_seqn_pair)
    ]
    nut_2_filtered[letter] = nut_2_datasets[letter][
        nut_2_datasets[letter]['SEQN'].isin(common_seqn_pair)
    ]
    
    # Print summary for this pair
    print(f"Cycle {letter}:")
    print(f"  Common SEQN values: {len(common_seqn_pair)}")
    print(f"  nut_1_{letter}: {nut_1_datasets[letter].shape} → {nut_1_filtered[letter].shape}")
    print(f"  nut_2_{letter}: {nut_2_datasets[letter].shape} → {nut_2_filtered[letter].shape}")
    print()

print("All cycle pairs processed!")
print("\nFiltered datasets are available as:")
for letter in cycle:
    print(f"  nut_1_{letter}_filtered and nut_2_{letter}_filtered")
```

    Processing each cycle pair:
    --------------------------------------------------
    Cycle C:
      Common SEQN values: 8213
      nut_1_C: (8214, 160) → (8213, 160)
      nut_2_C: (8220, 81) → (8213, 81)
    
    Cycle D:
      Common SEQN values: 8257
      nut_1_D: (8261, 160) → (8257, 160)
      nut_2_D: (8264, 81) → (8257, 81)
    
    Cycle E:
      Common SEQN values: 9762
      nut_1_E: (9762, 164) → (9762, 164)
      nut_2_E: (9762, 83) → (9762, 83)
    
    Cycle F:
      Common SEQN values: 8279
      nut_1_F: (8284, 166) → (8279, 166)
      nut_2_F: (8288, 83) → (8279, 83)
    
    Cycle G:
      Common SEQN values: 7486
      nut_1_G: (7486, 166) → (7486, 166)
      nut_2_G: (7496, 83) → (7486, 83)
    
    Cycle H:
      Common SEQN values: 7449
      nut_1_H: (7453, 168) → (7449, 168)
      nut_2_H: (7453, 85) → (7449, 85)
    
    Cycle I:
      Common SEQN values: 6863
      nut_1_I: (6864, 168) → (6863, 168)
      nut_2_I: (6875, 85) → (6863, 85)
    
    Cycle P:
      Common SEQN values: 10610
      nut_1_P: (10612, 168) → (10610, 168)
      nut_2_P: (10627, 85) → (10610, 85)
    
    Cycle L:
      Common SEQN values: 5828
      nut_1_L: (5828, 168) → (5828, 168)
      nut_2_L: (5830, 85) → (5828, 85)
    
    All cycle pairs processed!
    
    Filtered datasets are available as:
      nut_1_C_filtered and nut_2_C_filtered
      nut_1_D_filtered and nut_2_D_filtered
      nut_1_E_filtered and nut_2_E_filtered
      nut_1_F_filtered and nut_2_F_filtered
      nut_1_G_filtered and nut_2_G_filtered
      nut_1_H_filtered and nut_2_H_filtered
      nut_1_I_filtered and nut_2_I_filtered
      nut_1_P_filtered and nut_2_P_filtered
      nut_1_L_filtered and nut_2_L_filtered



```python
# rename the nutrient features to match data cycles

# Day 1 rename dictionary:
rename_dict_day1 = {
    'DR1TKCAL': 'Energy (kcal)',
    'DR1TPROT': 'Protein (gm)',
    'DR1TCARB': 'Carbohydrate (gm)',
    'DR1TSUGR': 'Total sugars (gm)',
    'DR1TFIBE': 'Dietary fiber (gm)',
    'DR1TTFAT': 'Total fat (gm)',
    'DR1TSFAT': 'Total saturated fatty acids (gm)',
    'DR1TMFAT': 'Total monounsaturated fatty acids (gm)',
    'DR1TPFAT': 'Total polyunsaturated fatty acids (gm)',
    'DR1TCHOL': 'Cholesterol (mg)',
    'DR1TATOC': 'Vitamin E as alpha-tocopherol (mg)',
    'DR1TATOA': 'Added alpha-tocopherol (Vitamin E) (mg)',
    'DR1TRET': 'Retinol (mcg)',
    'DR1TVARA': 'Vitamin A, RAE (mcg)',
    'DR1TACAR': 'Alpha-carotene (mcg)',
    'DR1TBCAR': 'Beta-carotene (mcg)',
    'DR1TCRYP': 'Beta-cryptoxanthin (mcg)',
    'DR1TLYCO': 'Lycopene (mcg)',
    'DR1TLZ': 'Lutein + zeaxanthin (mcg)',
    'DR1TVB1': 'Thiamin (Vitamin B1) (mg)',
    'DR1TVB2': 'Riboflavin (Vitamin B2) (mg)',
    'DR1TNIAC': 'Niacin (mg)',
    'DR1TVB6': 'Vitamin B6 (mg)',
    'DR1TFOLA': 'Total Folate (mcg)',
    'DR1TFA': 'Folic acid (mcg)',
    'DR1TFF': 'Food folate (mcg)',
    'DR1TFDFE': 'Folate, DFE (mcg)',
    'DR1TVB12': 'Vitamin B12 (mcg)',
    'DR1TB12A': 'Added vitamin B12 (mcg)',
    'DR1TVC': 'Vitamin C (mg)',
    'DR1TVK': 'Vitamin K (mcg)',
    'DR1TCALC': 'Calcium (mg)',
    'DR1TPHOS': 'Phosphorus (mg)',
    'DR1TMAGN': 'Magnesium (mg)',
    'DR1TIRON': 'Iron (mg)',
    'DR1TZINC': 'Zinc (mg)',
    'DR1TCOPP': 'Copper (mg)',
    'DR1TSODI': 'Sodium (mg)',
    'DR1TPOTA': 'Potassium (mg)',
    'DR1TSELE': 'Selenium (mcg)',
    'DR1TCAFF': 'Caffeine (mg)',
    'DR1TTHEO': 'Theobromine (mg)',
    'DR1TALCO': 'Alcohol (gm)',
    'DR1TMOIS': 'Moisture (gm)',
    'DR1TS040': 'SFA 4:0 (Butanoic) (gm)',
    'DR1TS060': 'SFA 6:0 (Hexanoic) (gm)',
    'DR1TS080': 'SFA 8:0 (Octanoic) (gm)',
    'DR1TS100': 'SFA 10:0 (Decanoic) (gm)',
    'DR1TS120': 'SFA 12:0 (Dodecanoic) (gm)',
    'DR1TS140': 'SFA 14:0 (Tetradecanoic) (gm)',
    'DR1TS160': 'SFA 16:0 (Hexadecanoic) (gm)',
    'DR1TS180': 'SFA 18:0 (Octadecanoic) (gm)',
    'DR1TM161': 'MFA 16:1 (Hexadecenoic) (gm)',
    'DR1TM181': 'MFA 18:1 (Octadecenoic) (gm)',
    'DR1TM201': 'MFA 20:1 (Eicosenoic) (gm)',
    'DR1TM221': 'MFA 22:1 (Docosenoic) (gm)',
    'DR1TP182': 'PFA 18:2 (Octadecadienoic) (gm)',
    'DR1TP183': 'PFA 18:3 (Octadecatrienoic) (gm)',
    'DR1TP184': 'PFA 18:4 (Octadecatetraenoic) (gm)',
    'DR1TP204': 'PFA 20:4 (Eicosatetraenoic) (gm)',
    'DR1TP205': 'PFA 20:5 (Eicosapentaenoic) (gm)',
    'DR1TP225': 'PFA 22:5 (Docosapentaenoic) (gm)',
    'DR1TP226': 'PFA 22:6 (Docosahexaenoic) (gm)'
}

# Day 2 rename dictionary
rename_dict_day2 = {
    'DR2TKCAL': 'Energy (kcal)',
    'DR2TPROT': 'Protein (gm)',
    'DR2TCARB': 'Carbohydrate (gm)',
    'DR2TSUGR': 'Total sugars (gm)',
    'DR2TFIBE': 'Dietary fiber (gm)',
    'DR2TTFAT': 'Total fat (gm)',
    'DR2TSFAT': 'Total saturated fatty acids (gm)',
    'DR2TMFAT': 'Total monounsaturated fatty acids (gm)',
    'DR2TPFAT': 'Total polyunsaturated fatty acids (gm)',
    'DR2TCHOL': 'Cholesterol (mg)',
    'DR2TATOC': 'Vitamin E as alpha-tocopherol (mg)',
    'DR2TATOA': 'Added alpha-tocopherol (Vitamin E) (mg)',
    'DR2TRET': 'Retinol (mcg)',
    'DR2TVARA': 'Vitamin A, RAE (mcg)',
    'DR2TACAR': 'Alpha-carotene (mcg)',
    'DR2TBCAR': 'Beta-carotene (mcg)',
    'DR2TCRYP': 'Beta-cryptoxanthin (mcg)',
    'DR2TLYCO': 'Lycopene (mcg)',
    'DR2TLZ': 'Lutein + zeaxanthin (mcg)',
    'DR2TVB1': 'Thiamin (Vitamin B1) (mg)',
    'DR2TVB2': 'Riboflavin (Vitamin B2) (mg)',
    'DR2TNIAC': 'Niacin (mg)',
    'DR2TVB6': 'Vitamin B6 (mg)',
    'DR2TFOLA': 'Total Folate (mcg)',
    'DR2TFA': 'Folic acid (mcg)',
    'DR2TFF': 'Food folate (mcg)',
    'DR2TFDFE': 'Folate, DFE (mcg)',
    'DR2TVB12': 'Vitamin B12 (mcg)',
    'DR2TB12A': 'Added vitamin B12 (mcg)',
    'DR2TVC': 'Vitamin C (mg)',
    'DR2TVK': 'Vitamin K (mcg)',
    'DR2TCALC': 'Calcium (mg)',
    'DR2TPHOS': 'Phosphorus (mg)',
    'DR2TMAGN': 'Magnesium (mg)',
    'DR2TIRON': 'Iron (mg)',
    'DR2TZINC': 'Zinc (mg)',
    'DR2TCOPP': 'Copper (mg)',
    'DR2TSODI': 'Sodium (mg)',
    'DR2TPOTA': 'Potassium (mg)',
    'DR2TSELE': 'Selenium (mcg)',
    'DR2TCAFF': 'Caffeine (mg)',
    'DR2TTHEO': 'Theobromine (mg)',
    'DR2TALCO': 'Alcohol (gm)',
    'DR2TMOIS': 'Moisture (gm)',
    'DR2TS040': 'SFA 4:0 (Butanoic) (gm)',
    'DR2TS060': 'SFA 6:0 (Hexanoic) (gm)',
    'DR2TS080': 'SFA 8:0 (Octanoic) (gm)',
    'DR2TS100': 'SFA 10:0 (Decanoic) (gm)',
    'DR2TS120': 'SFA 12:0 (Dodecanoic) (gm)',
    'DR2TS140': 'SFA 14:0 (Tetradecanoic) (gm)',
    'DR2TS160': 'SFA 16:0 (Hexadecanoic) (gm)',
    'DR2TS180': 'SFA 18:0 (Octadecanoic) (gm)',
    'DR2TM161': 'MFA 16:1 (Hexadecenoic) (gm)',
    'DR2TM181': 'MFA 18:1 (Octadecenoic) (gm)',
    'DR2TM201': 'MFA 20:1 (Eicosenoic) (gm)',
    'DR2TM221': 'MFA 22:1 (Docosenoic) (gm)',
    'DR2TP182': 'PFA 18:2 (Octadecadienoic) (gm)',
    'DR2TP183': 'PFA 18:3 (Octadecatrienoic) (gm)',
    'DR2TP184': 'PFA 18:4 (Octadecatetraenoic) (gm)',
    'DR2TP204': 'PFA 20:4 (Eicosatetraenoic) (gm)',
    'DR2TP205': 'PFA 20:5 (Eicosapentaenoic) (gm)',
    'DR2TP225': 'PFA 22:5 (Docosapentaenoic) (gm)',
    'DR2TP226': 'PFA 22:6 (Docosahexaenoic) (gm)'
}

# New column names for selection
new_column_names = [
    'SEQN',
    'Energy (kcal)',
    'Protein (gm)',
    'Carbohydrate (gm)',
    'Total sugars (gm)',
    'Dietary fiber (gm)',
    'Total fat (gm)',
    'Total saturated fatty acids (gm)',
    'Total monounsaturated fatty acids (gm)',
    'Total polyunsaturated fatty acids (gm)',
    'Cholesterol (mg)',
    'Vitamin E as alpha-tocopherol (mg)',
    'Added alpha-tocopherol (Vitamin E) (mg)',
    'Retinol (mcg)',
    'Vitamin A, RAE (mcg)',
    'Alpha-carotene (mcg)',
    'Beta-carotene (mcg)',
    'Beta-cryptoxanthin (mcg)',
    'Lycopene (mcg)',
    'Lutein + zeaxanthin (mcg)',
    'Thiamin (Vitamin B1) (mg)',
    'Riboflavin (Vitamin B2) (mg)',
    'Niacin (mg)',
    'Vitamin B6 (mg)',
    'Total Folate (mcg)',
    'Folic acid (mcg)',
    'Food folate (mcg)',
    'Folate, DFE (mcg)',
    'Vitamin B12 (mcg)',
    'Added vitamin B12 (mcg)',
    'Vitamin C (mg)',
    'Vitamin K (mcg)',
    'Calcium (mg)',
    'Phosphorus (mg)',
    'Magnesium (mg)',
    'Iron (mg)',
    'Zinc (mg)',
    'Copper (mg)',
    'Sodium (mg)',
    'Potassium (mg)',
    'Selenium (mcg)',
    'Caffeine (mg)',
    'Theobromine (mg)',
    'Alcohol (gm)',
    'Moisture (gm)',
    'SFA 4:0 (Butanoic) (gm)',
    'SFA 6:0 (Hexanoic) (gm)',
    'SFA 8:0 (Octanoic) (gm)',
    'SFA 10:0 (Decanoic) (gm)',
    'SFA 12:0 (Dodecanoic) (gm)',
    'SFA 14:0 (Tetradecanoic) (gm)',
    'SFA 16:0 (Hexadecanoic) (gm)',
    'SFA 18:0 (Octadecanoic) (gm)',
    'MFA 16:1 (Hexadecenoic) (gm)',
    'MFA 18:1 (Octadecenoic) (gm)',
    'MFA 20:1 (Eicosenoic) (gm)',
    'MFA 22:1 (Docosenoic) (gm)',
    'PFA 18:2 (Octadecadienoic) (gm)',
    'PFA 18:3 (Octadecatrienoic) (gm)',
    'PFA 18:4 (Octadecatetraenoic) (gm)',
    'PFA 20:4 (Eicosatetraenoic) (gm)',
    'PFA 20:5 (Eicosapentaenoic) (gm)',
    'PFA 22:5 (Docosapentaenoic) (gm)',
    'PFA 22:6 (Docosahexaenoic) (gm)'
]
```


```python
# rename columns and select the nutrients in common across all cyles:
nut_1_C_ = nut_1_filtered['C'].rename(columns=rename_dict_day1)
nut_1_C_selected = nut_1_C_[new_column_names]

nut_2_C_ = nut_2_filtered['C'].rename(columns=rename_dict_day2)
nut_2_C_selected = nut_2_C_[new_column_names]

nut_1_D_ = nut_1_filtered['D'].rename(columns=rename_dict_day1)
nut_1_D_selected = nut_1_D_[new_column_names]

nut_2_D_ = nut_2_filtered['D'].rename(columns=rename_dict_day2)
nut_2_D_selected = nut_2_D_[new_column_names]

nut_1_E_ = nut_1_filtered['E'].rename(columns=rename_dict_day1)
nut_1_E_selected = nut_1_E_[new_column_names]

nut_2_E_ = nut_2_filtered['E'].rename(columns=rename_dict_day2)
nut_2_E_selected = nut_2_E_[new_column_names]

nut_1_F_ = nut_1_filtered['F'].rename(columns=rename_dict_day1)
nut_1_F_selected = nut_1_F_[new_column_names]

nut_2_F_ = nut_2_filtered['F'].rename(columns=rename_dict_day2)
nut_2_F_selected = nut_2_F_[new_column_names]

nut_1_G_ = nut_1_filtered['G'].rename(columns=rename_dict_day1)
nut_1_G_selected = nut_1_G_[new_column_names]

nut_2_G_ = nut_2_filtered['G'].rename(columns=rename_dict_day2)
nut_2_G_selected = nut_2_G_[new_column_names]

nut_1_H_ = nut_1_filtered['H'].rename(columns=rename_dict_day1)
nut_1_H_selected = nut_1_H_[new_column_names]

nut_2_H_ = nut_2_filtered['H'].rename(columns=rename_dict_day2)
nut_2_H_selected = nut_2_H_[new_column_names]

nut_1_I_ = nut_1_filtered['I'].rename(columns=rename_dict_day1)
nut_1_I_selected = nut_1_I_[new_column_names]

nut_2_I_ = nut_2_filtered['I'].rename(columns=rename_dict_day2)
nut_2_I_selected = nut_2_I_[new_column_names]

nut_1_P_ = nut_1_filtered['P'].rename(columns=rename_dict_day1)
nut_1_P_selected = nut_1_P_[new_column_names]

nut_2_P_ = nut_2_filtered['P'].rename(columns=rename_dict_day2)
nut_2_P_selected = nut_2_P_[new_column_names]

nut_1_L_ = nut_1_filtered['L'].rename(columns=rename_dict_day1)
nut_1_L_selected = nut_1_L_[new_column_names]

nut_2_L_ = nut_2_filtered['L'].rename(columns=rename_dict_day2)
nut_2_L_selected = nut_2_L_[new_column_names]
```


```python
# combine all cycles
total_nut_all = pd.concat([nut_1_C_selected, nut_2_C_selected, nut_1_D_selected, nut_2_D_selected, nut_1_E_selected, nut_2_E_selected, nut_1_F_selected, nut_2_F_selected, nut_1_G_selected, nut_2_G_selected, nut_1_H_selected, nut_2_H_selected, nut_1_I_selected, nut_2_I_selected, nut_1_P_selected, nut_2_P_selected, nut_1_L_selected, nut_2_L_selected])

# average intakes for each particpant across their two 24-hr recalls
total_nut_mean = total_nut_all.groupby('SEQN').mean().reset_index()

# save the dataset
total_nut_mean.to_csv('../../data/00/wweia_dataset/wweia_total_nutrients_recalls_2023.csv', index=None)
```

### Generate WWEIA ingredients dataset using FDA disaggreagtion database


```python
# read in the FDA-FDD dataset (v3.0)
fdafdd =  pd.read_csv('../../data/00/ingredient_matching/FDA_FDD_All_Records_v_3.0.csv', usecols=['WWEIA Food Code', 'WWEIA Food Description', 'Basic Ingredient Description', 'Ingredient Percent'])

# rename columns for merging
fdafdd.rename(columns={'WWEIA Food Code': 'DRXFDCD'},inplace=True)

# get unique food codes from mixed meals dataset
unique_codes = WWEIA_ALL_final[['DRXFDCD', 'DRXFCLD']].drop_duplicates(subset='DRXFDCD')

# chage to integer for merging
unique_codes['DRXFDCD'] = unique_codes['DRXFDCD'].astype(int)

# merge the WWEIA food codes with those in the FDA database
unique_merged = unique_codes.merge(fdafdd, on='DRXFDCD', how='left')
```


```python
# check the shape of the dataset and see how many food codes were not matched (NAs)

print(unique_merged.shape[0] - unique_merged.dropna().shape[0], 'missing food code(s)') # 1 missing food code

# what was the missing foodcode?
missing_rows = unique_merged[unique_merged['DRXFDCD']==11836100]
print(missing_rows)
```

    1 missing food code(s)
            DRXFDCD                                            DRXFCLD  \
    39701  11836100  Protein supplement, milk-based, Muscle Milk Li...   
    
          WWEIA Food Description Basic Ingredient Description  Ingredient Percent  
    39701                    NaN                          NaN                 NaN  



```python
# generate WWEIA dataset for ingredient level intake using FDA-FDD
# first, merge the WWEIA dataset from above (food codes) with the FDA-FDD
wweia_fda = WWEIA_ALL_final.drop(columns='DRXFCLD').merge(fdafdd, on='DRXFDCD', how='left')

# Find rows where any of the specified columns has NA
columns_of_interest = ['DRXFDCD', 'WWEIA Food Description', 'Basic Ingredient Description', 'Ingredient Percent']
rows_with_na = wweia_fda[wweia_fda[columns_of_interest].isna().any(axis=1)]

# Display only these rows, focusing on the columns of interest
print(f"Found {len(rows_with_na)} rows with NAs in the specified columns")
print(rows_with_na[columns_of_interest])

# dropping NAs will remove the single foodcode 11836100 (Protein supplement)
wweia_fda = wweia_fda.dropna(subset=['DRXFDCD', 'WWEIA Food Description', 'Basic Ingredient Description', 'Ingredient Percent'])
```

    Found 5 rows with NAs in the specified columns
                DRXFDCD WWEIA Food Description Basic Ingredient Description  \
    3086571  11836100.0                    NaN                          NaN   
    3086572  11836100.0                    NaN                          NaN   
    3086759  11836100.0                    NaN                          NaN   
    3972678  11836100.0                    NaN                          NaN   
    4219184  11836100.0                    NaN                          NaN   
    
             Ingredient Percent  
    3086571                 NaN  
    3086572                 NaN  
    3086759                 NaN  
    3972678                 NaN  
    4219184                 NaN  



```python
# save unique fda ingredient list to match with FNDDS unique ingredient list using text matching algorithm
unique_ingred = wweia_fda[['Basic Ingredient Description']]
unique_ingred = unique_ingred['Basic Ingredient Description'].drop_duplicates()
unique_ingred.to_csv('../../data/00/ingredient_matching/wweia_fda_unique_ingredients.csv', index=None)
```


```python
# load in the data map for FDA to FNDDS (updated through 2023)
# Run scripts 00b and 00c for details on process, or load in the final mapping file here:

fda_map = pd.read_csv('../../data/00/ingredient_matching/fda_fndds_map_2023_final.csv')
```


```python
# rename the columns for merging
wweia_fda = wweia_fda.rename(columns={'Basic Ingredient Description': 'fda_desc'})

# select columns to keep
wweia_fda = wweia_fda[['SEQN', 'DRXFDCD', 'WWEIA Food Description', 'fda_desc',
       'Ingredient Percent', 'DR2ILINE', 'DR2IGRMS', 'diet_day',
       'diet_wts', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'RIDEXPRG', 'INDFMPIR',
       'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU',
       'SDMVSTRA', 'CYCLE']]

# merge the data with FDA-FDD
wweia_fda = wweia_fda.merge(fda_map, on='fda_desc', how='left')
```


```python
# How many missing rows?
print(round(wweia_fda['Ingredient description'].isna().sum()/(wweia_fda.shape[0]+wweia_fda['Ingredient description'].isna().sum())*100,ndigits=1), 'percent of ingredients (rows) not mapping from FNDDS to FDA-FDD')

# drop NAs for ingredients that didn't map from FDAFDD to FNDDS (mainly dehydrated ingredients)
wweia_fda = wweia_fda.dropna(subset='Ingredient description')
```

    6.6 percent of ingredients (rows) not mapping from FNDDS to FDA-FDD



```python
# dropping FDA descriptions, using the FNDDS descriptions with codes for the IR dataset
#wweia_fda = wweia_fda.drop(columns='fda_desc')
wweia_fda.rename(columns={'DRXFDCD': 'foodcode', 'WWEIA Food Description': 'food_description', 'Ingredient description': 'ingred_desc', 'Ingredient code': 'ingred_code', 'Ingredient Percent': 'ingred_wt'}, inplace=True)
```


```python
# Load ingredient nutrient value data, this will provide data on calories per ingredient consumed for adjusting by energy intake in the following script
ingred_nutrients = pd.read_csv('../../data/00/ingredient_matching/fndds_all_ingredient_nutrient_values_2023.csv', usecols=['Ingredient code', 'Ingredient description',  'Energy', 'Carbohydrate', 'Fiber, total dietary', 'Fatty acids, total monounsaturated', 'Fatty acids, total polyunsaturated', 'Fatty acids, total saturated', 'Sodium', 'Water'])

# rename column
ingred_nutrients.rename(columns={'Ingredient code': 'ingred_code'}, inplace=True)

# merge the nutrients composition data with out dietary dataset
wweia_complete_nutrients = pd.merge(wweia_fda, ingred_nutrients, on = 'ingred_code')
```


```python
# calculate the amount of each ingredient consumed and the quantity of each nutrient consumed per ingredient
# Convert the pandas dataframe to Dask dataframe
wweia_complete_nutrients_dd = dd.from_pandas(wweia_complete_nutrients, npartitions=10)

# Perform the same operations as before but in Dask
grouped_wt_sum_dd = wweia_complete_nutrients_dd.groupby(['SEQN', 'foodcode', 'DR2ILINE'])['ingred_wt'].sum().reset_index().rename(columns={'ingred_wt': 'ingred_wt_sum'})

# Merge this with the original dataframe.
merged_dd = dd.merge(wweia_complete_nutrients_dd, grouped_wt_sum_dd, on=['SEQN', 'foodcode', 'DR2ILINE'])

# Calculate the 'Ingred_consumed_g' using vectorized operations.
merged_dd['Ingred_consumed_g'] = merged_dd['DR2IGRMS'] * (merged_dd['ingred_wt'] / merged_dd['ingred_wt_sum'])

# Convert Dask dataframe back to pandas dataframe
wweia_all_recalls = merged_dd.compute()
```


```python
# calculate intakes for various nutrients. The primary nutrient we need is energy (kcal) to adjust intakes in following script
wweia_all_recalls.loc[:,['Energy']] = wweia_all_recalls.loc[:,['Energy']].multiply(wweia_all_recalls['Ingred_consumed_g'], axis=0) / 100
```


```python
# drop the ingredient description column that was added from merging with the ingredient nutrients data
wweia_all_recalls.drop(columns=['Ingredient description'], inplace=True)

# split metadata for combining with averaged recalls in next step
metadata = wweia_all_recalls.drop_duplicates(subset='SEQN')
metadata = metadata[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'RIDEXPRG', 'INDFMPIR',
       'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA',
       'CYCLE']]
```


```python
# average intake over 2 diet recall days (ingredients) for the IR dataset
# sum intakes
recalls_sum_ir = wweia_all_recalls.groupby(['SEQN', 'diet_day', 'foodcode', 'food_description', 'ingred_code', 'ingred_desc'])[['Ingred_consumed_g', 'Energy']].agg(np.sum).reset_index()
recalls_sum_ir.set_index(['SEQN', 'diet_day', 'foodcode', 'food_description', 'ingred_code', 'ingred_desc'],inplace=True)
r_sum_ir = recalls_sum_ir.unstack(level=['diet_day'], fill_value=0).stack()
r_sum_ir.reset_index(inplace=True)

# average intakes
recalls_mean_ir = r_sum_ir.groupby(['SEQN', 'foodcode', 'food_description', 'ingred_code', 'ingred_desc'])[['Ingred_consumed_g', 'Energy']].mean().reset_index()

# combine with metadata
recalls_mean_meta_ir = recalls_mean_ir.merge(metadata, on='SEQN', how='left')

# Save dataset
recalls_mean_meta_ir.to_csv('../../data/00/wweia_dataset/wweia_ingredients_recalls_2023.csv', index=None)
```

### now generate the data to map polyphenol intakes
- average intakes for two recall days
- scale intake per 1000kcal (other diet data are scaled in next script)


```python
# average intake over 2 diet recall days (ingredients)
# sum intakes
recalls_sum_poly = wweia_all_recalls.groupby(['SEQN', 'diet_day', 'foodcode', 'food_description', 'ingred_code', 'fda_desc'])[['Ingred_consumed_g', 'Energy']].agg(np.sum).reset_index()
recalls_sum_poly.set_index(['SEQN', 'diet_day', 'foodcode', 'food_description', 'ingred_code', 'fda_desc'],inplace=True)
r_sum_poly = recalls_sum_poly.unstack(level=['diet_day'], fill_value=0).stack()
r_sum_poly.reset_index(inplace=True)

# average intakes
recalls_mean_poly = r_sum_poly.groupby(['SEQN', 'foodcode', 'food_description', 'ingred_code', 'fda_desc'])[['Ingred_consumed_g', 'Energy']].mean().reset_index()

# combine with metadata
recalls_mean_meta_poly = recalls_mean_poly.merge(metadata, on='SEQN', how='left')

# scale food intake per 1000 kcal
recalls_mean_meta_poly['total_kcal_per_person'] = recalls_mean_meta_poly.groupby('SEQN')['Energy'].transform('sum')

# Then scale the gram intake to 1000 kcal
recalls_mean_meta_poly['ingredient_intake_g_per_1000kcal'] = (recalls_mean_meta_poly['Ingred_consumed_g'] / recalls_mean_meta_poly['total_kcal_per_person']) * 1000

# drop the intermediate columns
recalls_mean_meta_poly = recalls_mean_meta_poly.drop(['total_kcal_per_person', 'Ingred_consumed_g'], axis=1)

# Save dataset
recalls_mean_meta_poly.to_csv('../../data/00/wweia_dataset/wweia_ingredients_fda_desc_recalls_2023.csv', index=None)
```
