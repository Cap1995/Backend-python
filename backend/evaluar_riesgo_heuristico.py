import pandas as pd
import pyodbc
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()

# Conexión a SQL Server
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    f"UID={os.getenv('DB_USER')};"
    f"PWD={os.getenv('DB_PASSWORD')}"
)

# Consulta SQL que replica la lógica en C# con reemplazo de comas y validación ISNUMERIC
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
    e.[PROMEDIO AUTOEFICACIA ACADÉMICA],
    e.[PROMEDIO MODULACIÓN EMOCIONAL],
    e.[PROMEDIO AUTODETERMINACIÓN PERSONAL],
    e.[PROMEDIO SOCIABILIDAD],
    e.[PROMEDIO PROSPECTIVA ACADÉMICA]
FROM [dbo].[PACE2024_ACTUALIZADO] p
LEFT JOIN RamosConNotasCriticas r ON p.RUT = r.RUT
LEFT JOIN [dbo].[Epaes$] e ON CAST(p.RUT AS VARCHAR) = CAST(e.RUT AS VARCHAR)
"""

# Ejecutar consulta y cargar DataFrame
df = pd.read_sql(query, conn)

# Evaluación de riesgo según lógica heurística
def evaluar_riesgo(row):
    puntaje = 0
    motivos = []

    ramos = row["RamosReprobados"] if not pd.isna(row["RamosReprobados"]) else 0
    if ramos == 1:
        puntaje += 1
        motivos.append("1 ramo con nota menor a 4.0")
    elif ramos >= 2:
        puntaje += 2
        motivos.append(f"{int(ramos)} ramos con nota menor a 4.0")

    if pd.notna(row["PROMEDIO AUTOEFICACIA ACADÉMICA"]) and row["PROMEDIO AUTOEFICACIA ACADÉMICA"] < 3.0:
        puntaje += 1
        motivos.append("Autoeficacia baja")
    if pd.notna(row["PROMEDIO MODULACIÓN EMOCIONAL"]) and row["PROMEDIO MODULACIÓN EMOCIONAL"] < 3.0:
        puntaje += 1
        motivos.append("Modulación emocional baja")
    if pd.notna(row["PROMEDIO AUTODETERMINACIÓN PERSONAL"]) and row["PROMEDIO AUTODETERMINACIÓN PERSONAL"] < 3.0:
        puntaje += 1
        motivos.append("Autodeterminación baja")
    if pd.notna(row["PROMEDIO SOCIABILIDAD"]) and row["PROMEDIO SOCIABILIDAD"] < 3.0:
        puntaje += 1
        motivos.append("Sociabilidad baja")
    if pd.notna(row["PROMEDIO PROSPECTIVA ACADÉMICA"]) and row["PROMEDIO PROSPECTIVA ACADÉMICA"] < 3.0:
        puntaje += 1
        motivos.append("Prospectiva académica baja")

    nivel = "Bajo" if puntaje <= 1 else "Medio" if puntaje == 2 else "Alto"

    return pd.Series([puntaje, nivel, ", ".join(motivos)])

# Aplicar evaluación
df[["Puntaje", "NivelRiesgo", "Motivos"]] = df.apply(evaluar_riesgo, axis=1)

# Para usar en FastAPI
def obtener_riesgo_por_rut(rut: str):
    fila = df[df["RUT"] == rut]
    if fila.empty:
        return None
    fila = fila.iloc[0]
    return {
        "rut": rut,
        "nombre": fila["NOMBRE COMPLETO"],
        "carrera": fila["Carrera"],
        "riesgo": fila["NivelRiesgo"],
        "puntaje": int(fila["Puntaje"]),
        "motivos": fila["Motivos"]
    }
