---
layout: default
title: Generate Diet Representations
parent: Generate Dietary Datasets - md
nav_order: 3
has_toc: true
---

# Step 3: Generate Different Dietary Representations

## Table of Contents
{:toc}

<details open markdown="block">
  <summary>Show/hide TOC</summary>

  [Back to top](#step-3-generate-different-dietary-representations)

</details>

## Generate the Mixed Meal (foodcode level) dataset
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

## Generate FPED dataset


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

## Generate WWEIA total nutrients dataset


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

## Generate WWEIA ingredients dataset using FDA disaggreagtion database


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

