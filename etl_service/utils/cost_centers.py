def cost_centers(data_frame, catalog, code, id, cost_center, percentage_cost_center):
    cost_center_prorrateo = []
    cost_center_list = []
    
    for employee_id in data_frame[code]: #ME01010.000-ISGDDMFNN
        if employee_id in catalog[id].values:
            porcentajes = catalog.loc[catalog[id] == employee_id, percentage_cost_center].tolist()
            cc_values = catalog.loc[catalog[id] == employee_id, cost_center].tolist()
            
            for porcentaje, cc_value in zip(porcentajes, cc_values):
                if porcentaje > 0:  # Solo añadir si el porcentaje es mayor que 0
                    cost_center_prorrateo.append([employee_id, float(porcentaje) / 100, cc_value])
                else:
                    cost_center_list.append([employee_id, 1.0, None])  # Valor por defecto si no hay coincidencias
            
    return cost_center_prorrateo, cost_center_list