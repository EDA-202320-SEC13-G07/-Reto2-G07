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
 """

import config as cf
import model
import time
import csv

from DISClib.ADT import list as lt


"""
El controlador se encarga de mediar entre la vista y el modelo.
"""


def new_controller(choice):
    """
    Crea una instancia del modelo
    """
    #TODO: Llamar la función del modelo que crea las estructuras de datos
    Controlador = { "model": None}

    Controlador["model"] = model.new_data_structs(choice)

    return Controlador



# Funciones para la carga de datos

def load_data(control, size):
    """
    Carga los datos del reto
    """
    # TODO: Realizar la carga de datos

    control = control["model"]

    results = load_results(control,size)

    scorers = load_scorers(control,size)

    shootouts = load_shootouts(control,size)

    load_map_results_by_team(control,size)

    load_map_scorers(control,size)

    load_map_scorers_by_team(control,size)

    load_map_results_tournament(control,size)

    load_map_tournament_year(control,size)

    load_data_tutoria(control,size)

    return results,scorers,shootouts

#CARGA DE LA INFORMACION PARA LA PRESENTACION DE LA CARGA DE DATOS

def load_results(data_structs,size):
    data_results = cf.data_dir + "football/results-utf8-"+str(size)+".csv"
    input_file = csv.DictReader(open(data_results, encoding ="utf-8"))
    for result in input_file:
       result = model.new_data_results(result)
       model.add_data(data_structs["results"],result)
    return lt.size(data_structs["results"])

def load_scorers(data_structs,size):
    data_scorers = cf.data_dir + "football/goalscorers-utf8-"+str(size)+".csv"
    input_file = csv.DictReader(open(data_scorers, encoding ="utf-8"))
    for scorer in input_file:
       scorer = model.new_data_goalscorers(scorer)
       model.add_data(data_structs["goalscorers"],scorer)
    return lt.size(data_structs["goalscorers"])

def load_shootouts(data_structs,size):
    data_shootouts = cf.data_dir + "football/shootouts-utf8-"+str(size)+".csv"
    input_file = csv.DictReader(open(data_shootouts, encoding ="utf-8"))
    for shootout in input_file:
       model.add_data(data_structs["shootouts"],shootout)
    return lt.size(data_structs["shootouts"])

#CARGA DE LA INFORMACION EN LOS MAPAS

def load_map_results_by_team(data_structs,size):

    data_results = cf.data_dir + "football/results-utf8-"+str(size)+".csv"
    input_file = csv.DictReader(open(data_results, encoding ="utf-8"))
    for result in input_file:
        model.add_data_teams_map(data_structs,result)

def load_map_scorers(data_structs,size):

    data_scorers = cf.data_dir + "football/goalscorers-utf8-"+str(size)+".csv"
    input_file = csv.DictReader(open(data_scorers, encoding ="utf-8"))
    for scorer in input_file:
       scorer = model.new_data_goalscorers(scorer)
       model.add_data_goals_by_player(data_structs,scorer)

def load_map_scorers_by_team(data_structs,size):

    data_scorers = cf.data_dir + "football/goalscorers-utf8-"+str(size)+".csv"
    input_file = csv.DictReader(open(data_scorers, encoding ="utf-8"))
    for scorer in input_file:
       scorer = model.new_data_goalscorers(scorer)
       model.add_data_scorers_by_team(data_structs,scorer)

def load_map_results_tournament(data_structs,size):

    data_results = cf.data_dir + "football/results-utf8-"+str(size)+".csv"
    input_file = csv.DictReader(open(data_results, encoding ="utf-8"))
    for result in input_file:
        model.add_data_by_tournament(data_structs,result)

def load_map_tournament_year(data_structs,size):

    data_results = cf.data_dir + "football/results-utf8-"+str(size)+".csv"
    input_file = csv.DictReader(open(data_results, encoding ="utf-8"))
    for result in input_file:
        model.add_data_tournament_year(data_structs,result)


def load_data_tutoria(data_structs,size):

    data_results = cf.data_dir + "football/results-utf8-"+str(size)+".csv"
    input_file = csv.DictReader(open(data_results, encoding ="utf-8"))
    for result in input_file:
        model.add_data_map_tournament_year_teams(data_structs,result)



# Funciones de ordenamiento

def sort_data(control,order):
    """
    Ordena los datos del modelo
    """
    #TODO: Llamar la función del modelo para ordenar los datos

    return model.sort(control,order)


# Funciones de consulta sobre el catálogo

def get_data(control, id):
    """
    Retorna un dato por su ID.
    """
    #TODO: Llamar la función del modelo para obtener un dato
    pass


def req_1(control,equipo,condicion,n_numero):
    """
    Retorna el resultado del requerimiento 1
    """
    # TODO: Modificar el requerimiento 1
    return model.req_1(control,equipo,condicion,n_numero)


def req_2(control,nombre_jugador,n_numero):
    """
    Retorna el resultado del requerimiento 2
    """
    # TODO: Modificar el requerimiento 2
    return model.req_2(control["model"],nombre_jugador,n_numero)


def req_3(control,high_date,low_date,nombre_equipo):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    return model.req_3(control["model"],high_date,low_date,nombre_equipo)


def req_4(control,nombre_torneo,high_date,low_date):
    """
    Retorna el resultado del requerimiento 4
    """
    # TODO: Modificar el requerimiento 4
    return model.req_4(control["model"],nombre_torneo,high_date,low_date)


def req_5(control,nombre_jugador,high_date,low_date):
    """
    Retorna el resultado del requerimiento 5
    """
    # TODO: Modificar el requerimiento 5
    return model.req_5(control,nombre_jugador,high_date,low_date)

def req_6(control,nombre_torneo,anio,n_numero):
    """
    Retorna el resultado del requerimiento 6
    """
    # TODO: Modificar el requerimiento 6
    return model.req_6(control["model"],nombre_torneo,anio,n_numero)


def req_7(control,nombre_torneo,n_numero):
    """
    Retorna el resultado del requerimiento 7
    """
    # TODO: Modificar el requerimiento 7
    return model.req_7(control["model"],nombre_torneo,n_numero)


def req_8(control):
    """
    Retorna el resultado del requerimiento 8
    """
    # TODO: Modificar el requerimiento 8
    return 


# Funciones para medir tiempos de ejecucion

def get_time():
    """
    devuelve el instante tiempo de procesamiento en milisegundos
    """
    return float(time.perf_counter()*1000)


def delta_time(start, end):
    """
    devuelve la diferencia entre tiempos de procesamiento muestreados
    """
    elapsed = float(end - start)
    return elapsed