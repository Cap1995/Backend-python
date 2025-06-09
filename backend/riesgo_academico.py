def calcular_riesgo_academico(ramos: int):
    puntaje = 0
    motivo = ""

    if ramos == 1:
        puntaje = 1
        motivo = "1 ramo con nota menor a 4.0"
    elif ramos >= 2:
        puntaje = 2
        motivo = f"{ramos} ramos con nota menor a 4.0"
    
    nivel = "Bajo" if puntaje == 0 else "Medio" if puntaje == 1 else "Alto"
    
    return puntaje, nivel, motivo
