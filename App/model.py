"""
 * Copyright 2020, Departamento de sistemas y Computación,
 * Universidad de Los Andes
 *
 *
 * Desarrolado para el curso ISIS1225 - Estructuras de Datos y Algoritmos
 *
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along withthis program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contribuciones:
 *
 * Dario Correal - Version inicial
 """


import time
import config as cf
from DISClib.ADT import list as lt
from DISClib.ADT import stack as st
from DISClib.ADT import queue as qu
from DISClib.ADT import map as mp
from DISClib.DataStructures import mapentry as me
from DISClib.Algorithms.Sorting import shellsort as sa
from DISClib.Algorithms.Sorting import insertionsort as ins
from DISClib.Algorithms.Sorting import selectionsort as se
from DISClib.Algorithms.Sorting import mergesort as merg
from DISClib.Algorithms.Sorting import quicksort as quk
assert cf

"""
Se define la estructura de un catálogo de videos. El catálogo tendrá
dos listas, una para los videos, otra para las categorias de los mismos.
"""

# Construccion de modelos


def new_data_structs(choice):
    """
    Inicializa las estructuras de datos del modelo. Las crea de
    manera vacía para posteriormente almacenar la información.
    """
  
    data_structs = {"goalscorers": None,
                    "results": None,
                    "shootouts": None,
                    "teams_results": None,
                    "goals_by_player":None,
                    "scorers_by_team_name":None,
                    "results_by_tournament":None
                    }
    

    
    data_structs["goalscorers"] = lt.newList("ARRAY_LIST")
    data_structs["results"] = lt.newList("ARRAY_LIST")
    data_structs["shootouts"] = lt.newList("ARRAY_LIST")

    if choice == 1:

        data_structs["teams_results"] = mp.newMap(maptype="PROBING",loadfactor=0.5)
        data_structs["goals_by_player"] = mp.newMap(maptype="PROBING",loadfactor=0.5)
        data_structs["scorers_by_team_name"] = mp.newMap(maptype="PROBING",loadfactor=0.5)
        data_structs["results_by_tournament"] = mp.newMap(maptype="PROBING",loadfactor=0.5)
        data_structs["tournament_year"] = mp.newMap(maptype="PROBING",loadfactor=0.5)
        data_structs["tournament_year_teams"] = mp.newMap(maptype="PROBING",loadfactor=0.5)
        
        
    elif choice == 2:

        data_structs["teams_results"] = mp.newMap(maptype="CHAINING",loadfactor=4)
        data_structs["goals_by_player"] = mp.newMap(maptype="CHAINING",loadfactor=4)
        data_structs["scorers_by_team_name"] = mp.newMap(maptype="CHAINING",loadfactor=4)
        data_structs["results_by_tournament"] = mp.newMap(maptype="CHAINING",loadfactor=4)
        data_structs["tournament_year"] = mp.newMap(maptype="CHAINING",loadfactor=4)
        data_structs["tournament_year_teams"] = mp.newMap(maptype="CHAINING",loadfactor=4)

    return data_structs



# Funciones para agregar informacion al modelo

def add_data(data_structs, data):
    """
    Función para agregar nuevos elementos a la lista
    """
  
    
    lt.addLast(data_structs,data)


# Funciones para creacion de datos

def new_data_results(data):
    """
    Crea una nueva estructura para modelar los datos
    """

    data["away_score"] = int(data["away_score"])
    data["home_score"] = int(data["home_score"])
    
    return data

def new_data_goalscorers(data):

    if data["minute"] != "":

        data["minute"] = float(data["minute"])

    return data

#FUNCION QUE AGREGA LOS RESULTADOS DE CADA EQUIPO A UNA LISTA ADT EN UN MAPA

def add_data_teams_map(data_structs, data):

    team1 = data["home_team"]
    team2 = data["away_team"]

    entry1 = mp.get(data_structs["teams_results"],team1)

    entry2 = mp.get(data_structs["teams_results"],team2)

    if entry1:

        value1 = me.getValue(entry1)
        lt.addLast(value1["home"],data)
        lt.addLast(value1["all"],data)

    else:

        dict_values = {"home":None,"away":None,"all":None}

        dict_values["home"] = lt.newList("ARRAY_LIST")
        dict_values["away"] = lt.newList("ARRAY_LIST")
        dict_values["all"] = lt.newList("ARRAY_LIST")

        lt.addLast(dict_values["home"],data)
        lt.addLast(dict_values["all"],data)
        
        mp.put(data_structs["teams_results"],team1,dict_values)


    if entry2:

        value2 = me.getValue(entry2)

        lt.addLast(value2["away"],data)
        lt.addLast(value2["all"],data)
       
    else:

        dict_values2 = {"home":None,"away":None,"all":None}

        dict_values2["home"] = lt.newList("ARRAY_LIST")
        dict_values2["away"] = lt.newList("ARRAY_LIST")
        dict_values2["all"] = lt.newList("ARRAY_LIST")

        lt.addLast(dict_values2["away"],data)
        lt.addLast(dict_values2["all"],data)
        
        mp.put(data_structs["teams_results"],team2,dict_values2)
        

    return data_structs



#FUNCION QUE AGREGA LOS GOLES DE UN JUGADOR SEGUN SU NOMBRE

def add_data_goals_by_player(data_structs,data):

    name = data["scorer"]

    entry = mp.get(data_structs["goals_by_player"],name)

    if entry:

        value = me.getValue(entry)

        lt.addLast(value,data)

    else:

        value2 = lt.newList("ARRAY_LIST")

        lt.addLast(value2,data)

        mp.put(data_structs["goals_by_player"],name,value2)

    return data_structs


#FUNCION QUE AGREGA LOS SCORERS POR EQUIPO  


def add_data_scorers_by_team(data_structs,data):

    team = data["team"]

    entry = mp.get(data_structs["scorers_by_team_name"],team)

    if entry:

        value = me.getValue(entry)

        lt.addLast(value,data)

    else:

        value2 = lt.newList("ARRAY_LIST")

        lt.addLast(value2,data)

        mp.put(data_structs["scorers_by_team_name"],team,value2)

    return data_structs




#FUNCION QUE AGREGA LOS RESULTADOS POR TORNEO

def add_data_by_tournament(data_structs,data):

    map_results_tournament = data_structs["results_by_tournament"]

    tournament = data["tournament"]

    entry = mp.get(map_results_tournament,tournament)

    if entry:

        value = me.getValue(entry)

        lt.addLast(value,data)

    else:

        lst_value = lt.newList("ARRAY_LIST")

        lt.addLast(lst_value,data)

        mp.put(map_results_tournament,tournament,lst_value)

    return data_structs


#FUNCION QUE AGREGA LOS RESULTADOS POR TORNEO Y AÑO

def add_data_tournament_year(data_structs,data):

    map_tournament_year = data_structs["tournament_year"]

    entry = mp.get(map_tournament_year,data["tournament"])

    if entry:

        map_by_years = me.getValue(entry)

        entry_year = mp.get(map_by_years,data["date"][0:4])

        if entry_year:

            entry_year_list = me.getValue(entry_year)

            lt.addLast(entry_year_list,data)

        else:

            entry_year_list = lt.newList("ARRAY_LIST")

            lt.addLast(entry_year_list,data)

            mp.put(map_by_years,data["date"][0:4],entry_year_list)

    else:

        map_by_years = mp.newMap(maptype="PROBING",loadfactor=0.5)

        entry_year_list = lt.newList("ARRAY_LIST")

        lt.addLast(entry_year_list,data)

        mp.put(map_by_years,data["date"][0:4],entry_year_list)

        mp.put(map_tournament_year,data["tournament"],map_by_years)



    return data_structs


#FUNCIONES QUE SOLUCIONAN LOS REQUERIMIENTOS


def req_1(data_structs,equipo,condicion,n_numero):
    """
    Función que soluciona el requerimiento 1
    """
    
    map_results = data_structs["teams_results"]

    entry = mp.get(map_results,equipo)

    if condicion == "Local":
        condicion = "home"
    elif condicion == "Visitante":
        condicion = "away"
    elif condicion == "Indiferente":
        condicion = "all"

    if entry:

        lst_result = me.getValue(entry)[condicion]
    
    size = lt.size(lst_result)

    lst_result = merg.sort(lst_result,sort_criteria_results)
    
    if size > n_numero:
        
        ans = lt.subList(lst_result,1,n_numero)

    else:

        ans = lst_result

    return ans,size 







def req_2(data_structs,nombre_jugador,n_numero):

    """
    Función que soluciona el requerimiento 2
    """


    map_scorers = data_structs["goals_by_player"]

    entry = mp.get(map_scorers,nombre_jugador)

    contador = 0

    if entry:

        lst_result = me.getValue(entry)

    lst_result = merg.sort(lst_result,sort_criteria_goalscorers)

    size = lt.size(lst_result)

    if size > n_numero:

        ans = lt.subList(lst_result,1,n_numero)

    else:

        ans = lst_result

    for x in lt.iterator(ans):
        print(x)
        if x["penalty"] == "True":
            contador += 1

    return ans,size,contador




def req_3(data_structs,high_date,low_date,nombre_equipo):

    """
    Función que soluciona el requerimiento 3
    """

    map_results_by_team = data_structs["teams_results"]

    map_scorers_by_team = data_structs["scorers_by_team_name"]

    entry1 = mp.get(map_results_by_team,nombre_equipo)

    entry2 = mp.get(map_scorers_by_team,nombre_equipo)

    local = 0

    visitante = 0

    size = mp.size(map_results_by_team)

    if entry1 and entry2:

        value1 = me.getValue(entry1)["all"]

        value2 = me.getValue(entry2)

        lst_result = lt.newList("ARRAY_LIST")

        for team_result in lt.iterator(value1):
            if team_result["date"] >= low_date and team_result["date"] <= high_date:

                if team_result["home_team"] == nombre_equipo:
                    local += 1
                elif team_result["away_team"] == nombre_equipo:
                    visitante += 1

                team_result["penalty"] = "Unknown"
                team_result["own_goal"] = "Unknown"

                lt.addLast(lst_result,team_result)

        juegos = local + visitante


        for team_result_f in lt.iterator(lst_result):
            for team_scorer in lt.iterator(value2):
                if team_scorer["date"] == team_result_f["date"]:
                    if team_scorer["home_team"] == team_result_f["home_team"]:
                        if team_result_f["penalty"] != "True":
                            team_result_f["penalty"] = team_scorer["penalty"]
                        if team_result_f["own_goal"] != "True":
                            team_result_f["own_goal"] = team_scorer["own_goal"]

        lst_result = merg.sort(lst_result,sort_criteria_results)

        return lst_result,local,visitante,size,juegos
    

                        
def req_4(data_structs,nombre_torneo,high_date,low_date):
    
    results = data_structs["results_by_tournament"]

    shootouts = data_structs["shootouts"]

    entry = mp.get(results,nombre_torneo)

    tournaments = mp.size(results)

    countries = lt.newList("ARRAY_LIST")

    cities = lt.newList("ARRAY_LIST")

    lst_results = lt.newList("ARRAY_LIST")

    matches_tournament = 0

    shootout_counter = 0

    if entry:

        value = me.getValue(entry)

        

        for result_tournament in lt.iterator(value):

            if result_tournament["date"] >= low_date and  result_tournament["date"] <= high_date:

                if lt.isPresent(cities,result_tournament["city"]) == 0:

                    lt.addLast(cities,result_tournament["city"])

                if lt.isPresent(countries,result_tournament["country"]) == 0:

                    lt.addLast(countries,result_tournament["country"])

                result_tournament["winner"] = "Unavailable"

                matches_tournament += 1

                lt.addLast(lst_results,result_tournament)


        for result_tournament_filtered in lt.iterator(lst_results):
            for shootout in lt.iterator(shootouts):
                if shootout["date"] == result_tournament_filtered["date"]:
                    if shootout["home_team"] == result_tournament_filtered["home_team"]:
                        result_tournament_filtered["winner"] = shootout["winner"]
                        shootout_counter += 1
        
        lst_results = merg.sort(lst_results,sort_criteria_results)

    return lst_results,tournaments,matches_tournament,lt.size(cities),lt.size(countries),shootout_counter

        


def req_5(data_structs,nombre_jugador,high_date,low_date):
    """
    Función que soluciona el requerimiento 5
    """
    lista_scorers = data_structs["goals_by_player"]

    lista_results = data_structs["teams_results"]


    lista_entrega = lt.newList("ARRAY_LIST")

    contador_torneos = lt.newList("ARRAY_LIST")

    goles = 0

    penalties = 0

    autogoles = 0


    entry = mp.get(lista_scorers,nombre_jugador)

    if entry:

        value = me.getValue(entry)

        elemento = lt.getElement(value,1)

    team = elemento["team"]

    entry2 = mp.get(lista_results,team)

    if entry2:

        value2 = me.getValue(entry2)["all"]

    

    for scorer_filtrado in lt.iterator(value):
        if scorer_filtrado["date"] >= low_date and scorer_filtrado["date"] <= high_date:

            scorer_filtrado["tournament"] = "Unknown"

            if scorer_filtrado["own_goal"] == "True":
                autogoles += 1
            if scorer_filtrado["penalty"] == "True":
                penalties += 1

            

            goles += 1

            lt.addLast(lista_entrega,scorer_filtrado)

    for scorer_filtrado_entrega in lt.iterator(lista_entrega):
        for result_filtrado_team in lt.iterator(value2):
            if result_filtrado_team["date"] >= low_date and result_filtrado_team["date"] <= high_date:
                if result_filtrado_team["home_team"] == scorer_filtrado_entrega["home_team"]:
                    scorer_filtrado_entrega["tournament"] = result_filtrado_team["tournament"]
                    if lt.isPresent(contador_torneos,scorer_filtrado_entrega["tournament"]) == 0:
                        lt.addLast(contador_torneos,scorer_filtrado_entrega["tournament"])
                    

    lista_entrega = merg.sort(lista_entrega,sort_criteria_goalscorers)

    


    return lista_entrega,lt.size(contador_torneos),penalties,autogoles,goles



def comparacion_puntos(home_score,away_score):

    if home_score > away_score:
        points = 3
    elif home_score < away_score:
        points = 0
    else:
        points = 1

    return points



def comparacion_puntos_wins(puntos):

    victorias = 0
    perdidas = 0
    empates = 0

    if puntos == 3:
        victorias +=1 
    elif puntos == 1:
        empates +=1
    else:
        perdidas += 1
    
    return victorias,perdidas,empates


def cmp_mejor_equipo(equipo_1,equipo_2):
        
        "ordenar por mayor cantidad de puntos, mayor diferencia de goles, más goles por penal; en conjunto con la menor cantidad de disputas realizadas y menor número de autogoles."

        if equipo_1["total_points"] > equipo_2["total_points"]:
            return True
        elif equipo_1["total_points"] < equipo_2["total_points"]:
            return False
        else:
            if equipo_1["goal_diferrence"] > equipo_2["goal_diferrence"]:
                return True
            elif equipo_1["goal_diferrence"] < equipo_2["goal_diferrence"]:
                return False
            else:
                if equipo_1["penalty_points"] > equipo_2["penalty_points"]:
                    return True
                elif equipo_1["penalty_points"] < equipo_2["penalty_points"]:
                    return False
                else:
                    if equipo_1["matches"] < equipo_2["matches"]:
                        return True
                    elif equipo_1["matches"] > equipo_2["matches"]:
                        return False
                    else:
                        if equipo_1["own_goal_points"] < equipo_2["own_goal_points"]:
                            return True
                        elif equipo_1["own_goal_points"] > equipo_2["own_goal_points"]:
                            return False
                        else:
                            return False


        
        

def cmp_goals(scorer_1,scorer_2):

    if scorer_1["goals"] > scorer_2["goals"]:
        return True
    else:
        return False



def cmp_recent_goal(goal_1,goal_2):

    if goal_1["date"] < goal_2["date"]:
        return True
    else:
        return False
    

def cmp_best_player(scorer_1,scorer_2):

    if scorer_1["total_points"] > scorer_2["total_points"]:
        return True
    elif scorer_1["total_points"] < scorer_2["total_points"]:
        return False
    else:
        if scorer_1["total_goals"] > scorer_2["total_goals"]:
            return True
        elif scorer_1["total_goals"] < scorer_2["total_goals"]:
            return True
        else:
            if scorer_1["penalty_goals"] > scorer_2["penalty_goals"]:
                return True
            elif scorer_1["penalty_goals"] < scorer_2["penalty_goals"]:
                return False
            else:
                    if scorer_1["own_goals"] < scorer_2["own_goals"]:
                        return True
                    elif scorer_1["own_goals"] > scorer_2["own_goals"]:
                        return False
                    else:
                        if scorer_1["avg_time [min]"] < scorer_2["avg_time [min]"]:
                            return True
                        elif scorer_1["avg_time [min]"] > scorer_2["avg_time [min]"]:
                            return False
                        else:
                            return True
            

    
    
    

    
        
        


def req_7(data_structs,tournament,n_numero):
    """
    Función que soluciona el requerimiento 7-
    """
   
    map_results_tournament = data_structs["results_by_tournament"]

    goalscorers = data_structs["goalscorers"]

    entry = mp.get(map_results_tournament,tournament)

    tournaments = mp.size(map_results_tournament)

    goals = 0
    
    penalties = 0

    own_goals = 0

    if entry:

        value_list = me.getValue(entry)

    matches = lt.size(value_list)

    lst_players = lt.newList("ARRAY_LIST")
    lst_player_control = lt.newList("ARRAY_LIST")

    for player in lt.iterator(goalscorers):
        for result in lt.iterator(value_list):
            if result["date"] == player["date"]:
                if result["home_team"] == player["home_team"]:

                    goals += 1

                    entry = lt.isPresent(lst_player_control,player["scorer"])

                    if entry == 0:

                        new_player = {"scorer":None,
                                    "total_points":0,
                                    "total_goals":0,
                                    "penalty_goals":0,
                                    "own_goals":0,
                                    "avg_time [min]":0,
                                    "scored_in_wins":0,
                                    "scored_in_losses":0,
                                    "scored_in_draws":0,
                                    "total_time":0,
                                    "last_goal":lt.newList("ARRAY_LIST"),
                                    
                                    }
                        
                        new_player["scorer"] = player["scorer"]

                        new_player["total_time"] += float(player["minute"])

                        new_player["total_goals"] = 1 

                        if player["own_goal"] == "True":
                            new_player["own_goals"] += 1
                            own_goals += 1
                        if player["penalty"] == "True":
                            new_player["penalty_goals"] += 1
                            penalties += 1

                        if result["home_team"] == player["team"]:

                            if result["home_team"] > result["away_team"]:
                                new_player["scored_in_wins"] += 1
                            elif result["home_team"] < result["away_team"]:
                                new_player["scored_in_losses"] += 1
                            else:
                                new_player["scored_in_draws"] += 1

                        elif result["away_team"] == player["team"]:

                            if result["home_team"] < result["away_team"]:
                                new_player["scored_in_wins"] += 1
                            elif result["home_team"] > result["away_team"]:
                                new_player["scored_in_losses"] += 1
                            else:
                                new_player["scored_in_draws"] += 1

                       

                        new_player["avg_time [min]"] = new_player["total_time"] / new_player["total_goals"]
 
                        new_player["total_points"] = new_player["total_goals"] + new_player["penalty_goals"] - new_player["own_goals"]

                        player["tournament"] = result["tournament"]

                        lt.addLast(new_player["last_goal"],player)

                        lt.addLast(lst_players,new_player)

                        lt.addLast(lst_player_control,new_player["scorer"])

                    else:

                        goals += 1

                        player_found = lt.getElement(lst_players,entry)

                        player_found["total_time"] += float(player["minute"])

                        player_found["total_goals"] += 1 

                       

                        if player["own_goal"] == "True":
                            player_found["own_goals"] += 1
                            own_goals += 1
                        if player["penalty"] == "True":
                            player_found["penalty_goals"] += 1
                            penalties += 1

                        if result["home_team"] == player["team"]:

                            if result["home_team"] > result["away_team"]:
                                player_found["scored_in_wins"] += 1
                            elif result["home_team"] < result["away_team"]:
                                player_found["scored_in_losses"] += 1
                            else:
                                player_found["scored_in_draws"] += 1

                        elif result["away_team"] == player["team"]:

                            if result["home_team"] < result["away_team"]:
                                player_found["scored_in_wins"] += 1
                            elif result["home_team"] > result["away_team"]:
                                player_found["scored_in_losses"] += 1
                            else:
                                player_found["scored_in_draws"] += 1

                        player_found["avg_time [min]"] = player_found["total_time"] / player_found["total_goals"]
 
                        player_found["total_points"] = player_found["total_goals"] + player_found["penalty_goals"] - player_found["own_goals"]

                        player["tournament"] = result["tournament"]

                        lt.addLast(new_player["last_goal"],player)

                        


    lst_players_return = lt.newList("ARRAY_LIST")

    for player_filtered in lt.iterator(lst_players):
        if player_filtered["total_points"] == n_numero:
            if lt.size(player_filtered["last_goal"]) == 1:
                player_filtered["last_goal"] = lt.getElement(player_filtered["last_goal"],1)
            else:
                last_goal = merg.sort(player_filtered["last_goal"],cmp_recent_goal)
                last_goal = lt.getElement(last_goal,1)
                player_filtered["last_goal"] = last_goal
            lt.addLast(lst_players_return,player_filtered)

    lst_players_return = merg.sort(lst_players_return,cmp_best_player)
        
    return lst_players_return,tournaments,lt.size(lst_players),matches,goals,penalties,own_goals,lt.size(lst_players_return)






    



def req_8(data_structs):
    """
    Función que soluciona el requerimiento 8
    """
    # TODO: Realizar el requerimiento 8
    pass


# Funciones de ordenamiento


def sort_criteria_results(data_1, data_2):
    """sortCriteria criterio de ordenamiento para las funciones de ordenamiento

    Args:
        data1 (_type_): _description_
        data2 (_type_): _description_

    Returns:
        _type_: _description_
    """


    if data_1["date"] > data_2["date"]:
        return True
    elif data_1["date"] < data_2["date"]:
        return False
    else:
        if data_1["home_score"] > data_2["away_score"]:
            return True
        else:
            return False
  
def sort_criteria_shootouts(data_1, data_2):

    if data_1["date"] > data_2["date"]:
        return True
    elif data_1["date"] < data_2["date"]:
        return False
    else:
        if data_1["home_team"] > data_2["away_team"]:
            return True
        elif data_1["home_team"] < data_2["away_team"]:
            return False
        else:
            return False

def sort_criteria_goalscorers(data_1, data_2):
    
    if data_1["date"] > data_2["date"]:
        return True
    elif data_1["date"] < data_2["date"]:
        return False
    else:
        if data_1["minute"] > data_1["minute"]:
            return True
        elif data_1["minute"] < data_1["minute"]:
            return False
        else:
            if data_1["scorer"] > data_1["scorer"]:
                return True
            else:
                return False
        


def sort(data_structs,order):
    """
    Función encargada de ordenar la lista con los datos
    """
    #TODO: Crear función de ordenamiento
    if order == 1:
        data_structs["results"] = merg.sort(data_structs["results"],sort_criteria_results)
        data_structs["shootouts"] = merg.sort(data_structs["shootouts"],sort_criteria_shootouts)
        data_structs["goalscorers"] = merg.sort(data_structs["goalscorers"],sort_criteria_goalscorers)
    elif order == 2:
        data_structs["results"] = quk.sort(data_structs["results"],sort_criteria_results)
        data_structs["shootouts"] = quk.sort(data_structs["shootouts"],sort_criteria_shootouts)
        data_structs["goalscorers"] = quk.sort(data_structs["goalscorers"],sort_criteria_goalscorers)
    elif order == 3:
        data_structs["results"] = ins.sort(data_structs["results"],sort_criteria_results)
        data_structs["shootouts"] = ins.sort(data_structs["shootouts"],sort_criteria_shootouts)
        data_structs["goalscorers"] = ins.sort(data_structs["goalscorers"],sort_criteria_goalscorers)
    elif order == 4:
        data_structs["results"] = se.sort(data_structs["results"],sort_criteria_results)
        data_structs["shootouts"] = se.sort(data_structs["shootouts"],sort_criteria_shootouts)
        data_structs["goalscorers"] = se.sort(data_structs["goalscorers"],sort_criteria_goalscorers)

    return data_structs







def add_data_map_tournament_year_teams(data_structs,data):

    map_tournament_year = data_structs["tournament_year_teams"]

    tournament = data["tournament"]
    anio = data["date"][0:4]
    home_team = data["home_team"]
    away_team = data["away_team"]

    entry_year = mp.get(map_tournament_year,anio)

    if entry_year: # Existe la llave del anio en el mapa de anios

        year_tournament_map = me.getValue(entry_year)

        entry_tournament = mp.get(year_tournament_map,tournament) 

        if entry_tournament: #Existe la llave del torneo en el mapa del anio

            teams_tournament_map = me.getValue(entry_tournament)

            entry_home_team = mp.get(teams_tournament_map,home_team)

            entry_away_team = mp.get(teams_tournament_map,away_team)

            if entry_home_team: #Existe el equipo_home en el mapa del torneo del anio

                value_home_team = me.getValue(entry_home_team)

                lt.addLast(value_home_team,data)

            else: #No existe el equipo_home en el mapa del torneo del anio

                new_value_home_team = lt.newList("ARRAY_LIST")

                lt.addLast(new_value_home_team,data)

                mp.put(teams_tournament_map,home_team,new_value_home_team)


            if entry_away_team: #Existe el equipo_away en el mapa del torneo del anio

                value_away_team = me.getValue(entry_away_team)

                lt.addLast(value_away_team,data)

            else: #No existe el equipo_away en el mapa del torneo del anio

                new_value_away_team = lt.newList("ARRAY_LIST")

                lt.addLast(new_value_away_team,data)

                mp.put(teams_tournament_map,away_team,new_value_away_team)

        else: #No existe la llave del torneo en el mapa del anio

            new_teams_tournament_map = mp.newMap(maptype="PROBING",loadfactor=0.5)

            new_value_home_team = lt.newList("ARRAY_LIST")
            lt.addLast(new_value_home_team,data)

            new_value_away_team = lt.newList("ARRAY_LIST")
            lt.addLast(new_value_away_team,data)

            mp.put(new_teams_tournament_map,home_team,new_value_home_team)

            mp.put(new_teams_tournament_map,away_team,new_value_away_team)

            mp.put(year_tournament_map,tournament,new_teams_tournament_map)

    else: #No existe la llave del anio en el mapa de anios

        new_teams_tournament_map = mp.newMap(maptype="PROBING",loadfactor=0.5)

        new_year_tournament_map = mp.newMap(maptype="PROBING",loadfactor=0.5)

        new_value_home_team = lt.newList("ARRAY_LIST")
        lt.addLast(new_value_home_team,data)

        new_value_away_team = lt.newList("ARRAY_LIST")
        lt.addLast(new_value_away_team,data)

        mp.put(new_teams_tournament_map,home_team,new_value_home_team)

        mp.put(new_teams_tournament_map,away_team,new_value_away_team)

        mp.put(new_year_tournament_map,tournament,new_teams_tournament_map)

        mp.put(map_tournament_year,anio,new_year_tournament_map)

    return data_structs



def req_6(data_structs,tournament,anio,top):

    map_teams_torneo_anio = data_structs["tournament_year_teams"]

    goalscorers = data_structs["goalscorers"]

    entry_year = mp.get(map_teams_torneo_anio,anio)

    if entry_year:

        value_torneos_map = me.getValue(entry_year)

        entry_torneo = mp.get(value_torneos_map,tournament)

        if entry_torneo:

            value_teams_map = me.getValue(entry_torneo)

    tournaments = mp.size(value_torneos_map)

    cities = lt.newList("ARRAY_LIST")

    countries = lt.newList("ARRAY_LIST")

    matches = 0

    teams = mp.keySet(value_teams_map)

    teams_result = lt.newList("ARRAY_LIST")

    for team in lt.iterator(teams):
        team_list = me.getValue(mp.get(value_teams_map,team))

        new_team = {"team":team,
                        "total_points":0,
                        "goal_diferrence":0,
                        "penalty_points":0,
                        "matches":0,
                        "own_goal_points":0,
                        "wins":0,
                        "draws":0,
                        "losses":0,
                        "goals_for":0,
                        "goals_against":0,
                        "top_scorer":0}
        
        

        for team_result in lt.iterator(team_list):

            if lt.isPresent(cities,team_result["city"]) == 0:
                lt.addLast(cities,team_result["city"])

            if lt.isPresent(countries,team_result["country"]) == 0:
                lt.addLast(countries,team_result["country"])
            
            if team_result["home_team"] == team:

                matches += 1

                team_points = comparacion_puntos(team_result["home_score"],team_result["away_score"])

                new_team["total_points"] += team_points

                wins,losses,draws = comparacion_puntos_wins(team_points)

                new_team["wins"] = wins

                new_team["losses"] = losses

                new_team["draws"] = draws

                new_team["goals_for"] = int(team_result["home_score"])

                new_team["goals_against"] = int(team_result["away_score"])

                new_team["goal_diferrence"] = new_team["goals_for"] - new_team["goals_against"]

                new_team["matches"] += 1

            elif team_result["away_team"] == team:

                matches += 1

                team_points = comparacion_puntos(team_result["away_score"],team_result["home_score"])

                new_team["total_points"] += team_points

                wins,losses,draws = comparacion_puntos_wins(team_points)

                new_team["wins"] = wins

                new_team["losses"] = losses

                new_team["draws"] = draws

                new_team["goals_for"] = int(team_result["away_score"])

                new_team["goals_against"] = int(team_result["home_score"])

                new_team["goal_diferrence"] = new_team["goals_for"] - new_team["goals_against"]

                new_team["matches"] += 1

        lt.addLast(teams_result,new_team)


    for team_filtrado in lt.iterator(teams_result):
        team_players = lt.newList("ARRAY_LIST")
        team_players_control = lt.newList("ARRAY_LIST")
        for goalscorer in lt.iterator(goalscorers):
            if goalscorer["date"][0:4] == anio:
                if goalscorer["team"] == team_filtrado["team"]:

                    if goalscorer["own_goal"] == "True":
                        team_filtrado["own_goal_points"] += 1
                    if goalscorer["penalty"] == "True":
                        team_filtrado["penalty_points"] += 1

                    entry = lt.isPresent(team_players_control,goalscorer["scorer"])

                    if entry == 0:

                        new_scorer = {"scorer":None,
                                      "avg_time":None,
                                      "goals":None,
                                      "matches":None,
                                      "total_time":None}
                        
                        new_scorer["scorer"] = goalscorer["scorer"]

                        new_scorer["goals"] = 1

                        new_scorer["matches"] = 1

                        new_scorer["total_time"] = float(goalscorer["minute"])

                        new_scorer["avg_time"] = new_scorer["total_time"] / new_scorer["goals"]

                        lt.addLast(team_players_control,goalscorer["scorer"])

                        lt.addLast(team_players,new_scorer)

                    else:

                        scorer = lt.getElement(team_players,entry)

                        scorer["goals"] += 1

                        scorer["matches"] += 1

                        scorer["total_time"] += float(goalscorer["minute"])

                        scorer["avg_time"] += scorer["total_time"] / scorer["goals"]

        if lt.size(team_players) == 0:

            team_filtrado["top_scorer"] = {"scorer":"Unavailable",
                                      "avg_time":0,
                                      "goals":0,
                                      "matches":0,
                                      "total_time":0}
            
                        
        else:

            max_player_team = merg.sort(team_players,cmp_goals)
            max_player_team = lt.getElement(max_player_team,1)
            team_filtrado["top_scorer"] = max_player_team

    
    teams_result = merg.sort(teams_result,cmp_mejor_equipo)              

    teams = lt.size(teams_result)

    if teams > top:

        teams_result = lt.subList(teams_result,1,top)


    countries = lt.size(countries)

    cities = lt.size(cities)


    return teams_result,cities,countries,teams,matches,tournaments