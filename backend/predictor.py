import joblib
import pandas as pd
from backend.db import get_connection

# Cargar modelo entrenado
modelo = joblib.load("modelo_riesgo.pkl")

def predecir_riesgo_por_rut(rut: str):
    conn = get_connection()

    query = f"""
    SELECT 
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
    WHERE 
        n.RUT = ?
    GROUP BY 
        e.[PROMEDIO AUTOEFICACIA ACADÉMICA],
        e.[PROMEDIO AUTODETERMINACIÓN PERSONAL],
        e.[PROMEDIO MODULACIÓN EMOCIONAL],
        e.[PROMEDIO SOCIABILIDAD],
        e.[PROMEDIO ANTICIPACIÓN ANALÍTICA],
        e.[PROMEDIO PROSPECTIVA ACADÉMICA],
        e.[PROMEDIO COMUNICACIÓN EFECTIVA]
    """

    df = pd.read_sql(query, conn, params=[rut])

    if df.empty:
        return None

    # Calcular promedio solo de las notas disponibles
    nota_cols = ["Nota_1", "Nota_2", "Nota_3", "Nota_4", "Nota_5", "Nota_6"]
    df["promedio_notas"] = df[nota_cols].apply(lambda row: row.dropna().mean(), axis=1)

    # Reordenar columnas exactamente como durante el entrenamiento
    columnas_modelo = [
        "Nota_1", "Nota_2", "Nota_3", "Nota_4", "Nota_5", "Nota_6",
        "PROMEDIO AUTOEFICACIA ACADÉMICA",
        "PROMEDIO AUTODETERMINACIÓN PERSONAL",
        "PROMEDIO MODULACIÓN EMOCIONAL",
        "PROMEDIO SOCIABILIDAD",
        "PROMEDIO ANTICIPACIÓN ANALÍTICA",
        "PROMEDIO PROSPECTIVA ACADÉMICA",
        "PROMEDIO COMUNICACIÓN EFECTIVA",
        "promedio_notas"
    ]

    df = df[columnas_modelo]  # Asegura que el orden y nombres coincidan

    # Predecir
    prob = modelo.predict_proba(df)[0][1]
    clase = modelo.predict(df)[0]
    nivel = "Alto" if prob >= 0.75 else "Medio" if prob >= 0.5 else "Bajo"

    return {
        "rut": rut,
        "riesgo": nivel,
        "probabilidad": round(prob, 2),
        "clase": int(clase)
    }
