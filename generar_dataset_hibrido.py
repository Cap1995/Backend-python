import pandas as pd
import pyodbc
import os
from dotenv import load_dotenv

# Cargar variables de entorno (.env)
load_dotenv()

# Conexión a SQL Server
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    f"UID={os.getenv('DB_USER')};"
    f"PWD={os.getenv('DB_PASSWORD')}"
)

# Consulta SQL con lógica de ramos reprobados
query = """
WITH RamosConNotasCriticas AS (
    SELECT 
        RUT,
        COUNT(DISTINCT [Denominación Actividad Curricular]) AS RamosReprobados
    FROM [dbo].[NotasPace2025]
    CROSS APPLY (
        SELECT 
            CASE WHEN ISNUMERIC(REPLACE([Nota_1], ',', '.')) = 1 THEN CAST(REPLACE([Nota_1], ',', '.') AS FLOAT) ELSE NULL END UNION ALL
            SELECT CASE WHEN ISNUMERIC(REPLACE([Nota_2], ',', '.')) = 1 THEN CAST(REPLACE([Nota_2], ',', '.') AS FLOAT) ELSE NULL END UNION ALL
            SELECT CASE WHEN ISNUMERIC(REPLACE([Nota_3], ',', '.')) = 1 THEN CAST(REPLACE([Nota_3], ',', '.') AS FLOAT) ELSE NULL END UNION ALL
            SELECT CASE WHEN ISNUMERIC(REPLACE([Nota_4], ',', '.')) = 1 THEN CAST(REPLACE([Nota_4], ',', '.') AS FLOAT) ELSE NULL END UNION ALL
            SELECT CASE WHEN ISNUMERIC(REPLACE([Nota_5], ',', '.')) = 1 THEN CAST(REPLACE([Nota_5], ',', '.') AS FLOAT) ELSE NULL END UNION ALL
            SELECT CASE WHEN ISNUMERIC(REPLACE([Nota_6], ',', '.')) = 1 THEN CAST(REPLACE([Nota_6], ',', '.') AS FLOAT) ELSE NULL END
    ) AS Notas(nota)
    WHERE nota IS NOT NULL AND nota < 4.0
    GROUP BY RUT
)
SELECT 
    p.RUT,
    p.[NOMBRE COMPLETO],
    p.Carrera,
    ISNULL(r.RamosReprobados, 0) AS RamosReprobados,
    e.[PROMEDIO AUTOEFICACIA ACADÉMICA] AS Autoeficacia,
    e.[PROMEDIO MODULACIÓN EMOCIONAL] AS Emocional,
    e.[PROMEDIO AUTODETERMINACIÓN PERSONAL] AS Autodeterminacion,
    e.[PROMEDIO SOCIABILIDAD] AS Sociabilidad,
    e.[PROMEDIO PROSPECTIVA ACADÉMICA] AS Prospectiva
FROM [dbo].[PACE2024_ACTUALIZADO] p
LEFT JOIN RamosConNotasCriticas r ON p.RUT = r.RUT
LEFT JOIN [dbo].[Epaes$] e ON CAST(p.RUT AS VARCHAR) = CAST(e.RUT AS VARCHAR)
"""

# Cargar los datos
df = pd.read_sql(query, conn)

# Aplicar lógica heurística
def evaluar_heuristica(row):
    puntaje = 0

    ramos = row["RamosReprobados"] if not pd.isna(row["RamosReprobados"]) else 0
    if ramos == 1:
        puntaje += 1
    elif ramos >= 2:
        puntaje += 2

    if pd.notna(row["Autoeficacia"]) and row["Autoeficacia"] < 3.0:
        puntaje += 1
    if pd.notna(row["Emocional"]) and row["Emocional"] < 3.0:
        puntaje += 1
    if pd.notna(row["Autodeterminacion"]) and row["Autodeterminacion"] < 3.0:
        puntaje += 1
    if pd.notna(row["Sociabilidad"]) and row["Sociabilidad"] < 3.0:
        puntaje += 1
    if pd.notna(row["Prospectiva"]) and row["Prospectiva"] < 3.0:
        puntaje += 1

    if puntaje <= 1:
        nivel = 0  # Bajo
    elif puntaje == 2:
        nivel = 1  # Medio
    else:
        nivel = 2  # Alto

    return pd.Series([puntaje, nivel])

# Agregar columnas al DataFrame
df[["PuntajeHeuristico", "NivelHeuristico"]] = df.apply(evaluar_heuristica, axis=1)

# Guardar CSV
df.to_csv("dataset_hibrido.csv", index=False, encoding="utf-8-sig")
print("✅ Dataset guardado como dataset_hibrido.csv")
