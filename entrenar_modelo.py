import pandas as pd
import pyodbc
from dotenv import load_dotenv
import os
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import joblib

# Cargar variables .env
load_dotenv()

# Conexión a SQL Server
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    f"UID={os.getenv('DB_USER')};"
    f"PWD={os.getenv('DB_PASSWORD')}"
)

# Consulta SQL
query = """
SELECT 
    n.RUT,
    AVG(TRY_CAST(n.Nota_1 AS FLOAT)) AS Nota_1,
    AVG(TRY_CAST(n.Nota_2 AS FLOAT)) AS Nota_2,
    AVG(TRY_CAST(n.Nota_3 AS FLOAT)) AS Nota_3,
    AVG(TRY_CAST(n.Nota_4 AS FLOAT)) AS Nota_4,
    AVG(TRY_CAST(n.Nota_5 AS FLOAT)) AS Nota_5,
    AVG(TRY_CAST(n.Nota_6 AS FLOAT)) AS Nota_6,
    e.[PROMEDIO AUTOEFICACIA ACADÉMICA],
    e.[PROMEDIO AUTODETERMINACIÓN PERSONAL],
    e.[PROMEDIO MODULACIÓN EMOCIONAL],
    e.[PROMEDIO SOCIABILIDAD],
    e.[PROMEDIO ANTICIPACIÓN ANALÍTICA],
    e.[PROMEDIO PROSPECTIVA ACADÉMICA],
    e.[PROMEDIO COMUNICACIÓN EFECTIVA]
FROM 
    dbo.NotasPace2025 n
INNER JOIN 
    dbo.[Epaes$] e ON n.RUT = e.RUT
GROUP BY 
    n.RUT, 
    e.[PROMEDIO AUTOEFICACIA ACADÉMICA],
    e.[PROMEDIO AUTODETERMINACIÓN PERSONAL],
    e.[PROMEDIO MODULACIÓN EMOCIONAL],
    e.[PROMEDIO SOCIABILIDAD],
    e.[PROMEDIO ANTICIPACIÓN ANALÍTICA],
    e.[PROMEDIO PROSPECTIVA ACADÉMICA],
    e.[PROMEDIO COMUNICACIÓN EFECTIVA]
"""

# Leer datos
df = pd.read_sql(query, conn)

# Calcular promedio parcial de notas
nota_cols = ["Nota_1", "Nota_2", "Nota_3", "Nota_4", "Nota_5", "Nota_6"]
df["promedio_notas"] = df[nota_cols].apply(lambda row: row.dropna().mean(), axis=1)

# Forzar riesgo a los peores 10% promedios
df = df.sort_values("promedio_notas", ascending=True).reset_index(drop=True)
top_n = max(int(len(df) * 0.10), 1)  # al menos 1 estudiante en riesgo
df["riesgo"] = 0
df.loc[:top_n - 1, "riesgo"] = 1

# Mostrar distribución
print("📊 Distribución de clases:")
print(df["riesgo"].value_counts())

# Prevenir entrenamiento si solo hay una clase
if df["riesgo"].nunique() < 2:
    print("⚠️ No hay suficientes clases distintas para entrenar el modelo.")
    exit()

# Entrenamiento
X = df.drop(columns=["RUT", "riesgo"])
y = df["riesgo"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

modelo = RandomForestClassifier(n_estimators=100, random_state=42)
modelo.fit(X_train, y_train)

# Evaluación
print("\n🧪 Evaluación del modelo:")
print(classification_report(y_test, modelo.predict(X_test)))

# Guardar modelo
joblib.dump(modelo, "modelo_riesgo.pkl")
print("\n✅ Modelo guardado como modelo_riesgo.pkl")
