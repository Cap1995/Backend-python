def calcular_riesgo_psicologico(autoeficacia, emocional, autodeterminacion, sociabilidad, prospectiva):
    puntaje = 0
    motivos = []

    if autoeficacia is not None and autoeficacia < 3.0:
        puntaje += 1
        motivos.append("Autoeficacia baja")
    if emocional is not None and emocional < 3.0:
        puntaje += 1
        motivos.append("Modulación emocional baja")
    if autodeterminacion is not None and autodeterminacion < 3.0:
        puntaje += 1
        motivos.append("Autodeterminación baja")
    if sociabilidad is not None and sociabilidad < 3.0:
        puntaje += 1
        motivos.append("Sociabilidad baja")
    if prospectiva is not None and prospectiva < 3.0:
        puntaje += 1
        motivos.append("Prospectiva académica baja")

    nivel = "Bajo" if puntaje <= 1 else "Medio" if puntaje == 2 else "Alto"
    return puntaje, nivel, ", ".join(motivos)
