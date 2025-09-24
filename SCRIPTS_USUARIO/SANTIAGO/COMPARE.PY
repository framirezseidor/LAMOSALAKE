import pandas as pd
 
df1 = pd.read_csv("C:\\Users\\Seidor Analytics\\OneDrive - SEIDOR ANALYTICS\\LAMOSA - Documentos\\RSA3_2025002_VERS.csv")
df2 = pd.read_csv("C:\\Users\\Seidor Analytics\\OneDrive - SEIDOR ANALYTICS\\LAMOSA - Documentos\\EXTRACT_GASTOSPRES_2025002.csv")
 
# Compare DataFrames
res = df1.compare(df2)
print(res)
