---
layout: default
title: Harmonize Data Across Cycles
parent: Generate Dietary Datasets - md
nav_order: 2
has_toc: true
---

# Step 2: Harmonization of Dietary Data Across Multiple NHANES cycles

### Cross walk food codes and descriptions to harmonize data across cycles

NHANES uses the Food and Nutrient Database for Dietary Studies (FNDDS) to generate nutrient intakes from food composition data. FNDDS is released every two-years in conjunction with the WWEIA, NHANES dietary data release. For each new version of FNDDS, foods/beverages, portions, and nutrient values are reviewed and updated. As we are combining many NHANES/WWEIA cycles, we are going to use the cross-walk to provide the most updated versions of FNDDS foods and beverages to harmonize the changes over the cycle years


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
