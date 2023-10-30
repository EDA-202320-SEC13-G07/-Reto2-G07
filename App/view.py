"""
 * Copyright 2020, Departamento de sistemas y Computación, Universidad
 * de Los Andes
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
import sys
import controller
from DISClib.ADT import list as lt
from DISClib.ADT import stack as st
from DISClib.ADT import queue as qu
assert cf
from tabulate import tabulate
import traceback

"""
La vista se encarga de la interacción con el usuario
Presenta el menu de opciones y por cada seleccion
se hace la solicitud al controlador para ejecutar la
operación solicitada
"""


def tabulate_transformations(lista):

    lista_grande = []

    if lt.size(lista) > 6:

        primeros_tres = lt.subList(lista,1,3)
        ultimos_tres = lt.subList(lista,lt.size(lista)-2,3)
        for p3 in lt.iterator(primeros_tres):
            keys = list(p3.keys())
            lista_ligera = list(p3.values())
            lista_grande.append(lista_ligera)
        for l3 in lt.iterator(ultimos_tres):
            lista_ligera = list(l3.values())
            lista_grande.append(lista_ligera)
    
    else:

        for elemento in lt.iterator(lista):
            keys = list(elemento.keys())
            lista_ligera = list(elemento.values())
            lista_grande.append(lista_ligera)


    return lista_grande,keys


def tabulate_transformation_req6(lista):

    lista_grande = []

    if lt.size(lista) > 6:

        primeros_tres = lt.subList(lista,1,3)
        ultimos_tres = lt.subList(lista,lt.size(lista)-2,3)
        
        for p3 in lt.iterator(primeros_tres):

            lista_interna = []
            keys_internas = list(p3["top_scorer"].keys())
            values = list(p3["top_scorer"].values())
            lista_interna.append(values)


            p3["top_scorer"] = tabulate(lista_interna,keys_internas,tablefmt="grid")
            keys = list(p3.keys())
            lista_ligera = list(p3.values())
            lista_grande.append(lista_ligera)


        for l3 in lt.iterator(ultimos_tres):


            lista_interna = []
            keys_internas = list(l3["top_scorer"].keys())
            values = list(l3["top_scorer"].values())
            lista_interna.append(values)


            l3["top_scorer"] = tabulate(lista_interna,keys_internas,tablefmt="grid")
            keys = list(l3.keys())
            lista_ligera = list(l3.values())
            lista_grande.append(lista_ligera)
    
    else:

        for elemento in lt.iterator(lista):
            lista_interna = []
            keys_internas = list(elemento["top_scorer"].keys())
            values = list(elemento["top_scorer"].values())
            lista_interna.append(values)
            elemento["top_scorer"] = tabulate(lista_interna,keys_internas,tablefmt="grid")
            keys = list(elemento.keys())
            lista_ligera = list(elemento.values())
            lista_grande.append(lista_ligera)

    return lista_grande,keys


def tabulate_transformation_req7(lista):

    lista_grande = []

    if lt.size(lista) > 6:

        primeros_tres = lt.subList(lista,1,3)
        ultimos_tres = lt.subList(lista,lt.size(lista)-2,3)
        for p3 in lt.iterator(primeros_tres):
            lista_interna = []
            keys_internas = list(p3["last_goal"].keys())
            values = list(p3["last_goal"].values())
            lista_interna.append(values)
            p3["last_goal"] = tabulate(lista_interna,keys_internas,tablefmt="grid")
            keys = list(p3.keys())
            lista_ligera = list(p3.values())
            lista_grande.append(lista_ligera)
        for l3 in lt.iterator(ultimos_tres):
            lista_interna = []
            keys_internas = list(l3["last_goal"].keys())
            values = list(l3["last_goal"].values())
            lista_interna.append(values)
            l3["last_goal"] = tabulate(lista_interna,keys_internas,tablefmt="grid")
            keys = list(l3.keys())
            lista_ligera = list(l3.values())
            lista_grande.append(lista_ligera)
    
    else:

        for elemento in lt.iterator(lista):
            lista_interna = []
            keys_internas = list(elemento["last_goal"].keys())
            values = list(elemento["last_goal"].values())
            lista_interna.append(values)
            elemento["last_goal"] = tabulate(lista_interna,keys_internas,tablefmt="grid")
            keys = list(elemento.keys())
            lista_ligera = list(elemento.values())
            lista_grande.append(lista_ligera)

    return lista_grande,keys





def new_controller(choice):
    """
        Se crea una instancia del controlador
    """
    
    controlador = controller.new_controller(choice)

    return controlador


def print_menu():
    print("Bienvenido")
    print("1- Cargar información")
    print("2- Ejecutar Requerimiento 1")
    print("3- Ejecutar Requerimiento 2")
    print("4- Ejecutar Requerimiento 3")
    print("5- Ejecutar Requerimiento 4")
    print("6- Ejecutar Requerimiento 5")
    print("7- Ejecutar Requerimiento 6")
    print("8- Ejecutar Requerimiento 7")
    print("9- Ejecutar Requerimiento 8")
    print("0- Salir")


def load_data(control,size):
    """
    Carga los datos
    """
    results,scorers,shootouts = controller.load_data(control,size)
    
    return results,scorers,shootouts



def print_req_1(control,equipo,condicion,n_numero):
    """
        Función que imprime la solución del Requerimiento 1 en consola
    """
    

    requerimiento_1,size_real_req1 = controller.req_1(control,equipo,condicion,n_numero)

    lista_tabulate_req_1,keys_req_1 = tabulate_transformations(requerimiento_1)

    print("================REQ-1================")

    print("\n")

    print("Se buscaron",n_numero,"partidos del equipo",equipo) 

    print("\n")

    print("Se encontraron",size_real_req1,"partidos para los parametros ingresados") 

    print("\n")

    print("=================TABLE=================")

    print(tabulate(lista_tabulate_req_1,keys_req_1,tablefmt="fancy_outline"))


def print_req_2(control,nombre_jugador,n_numero):
    """
        Función que imprime la solución del Requerimiento 2 en consola
    """
    

    requerimiento_2,size_real_req2,counter_req2 = controller.req_2(control,nombre_jugador,n_numero)

    lista_tabulate_req2,keys_req2 = tabulate_transformations(requerimiento_2)

    print("================REQ-2================")

    print("\n")

    print("Se buscaron",n_numero,"anotaciones del jugador",nombre_jugador) 

    print("\n")

    print("Se encontraron",size_real_req2,"anotaciones del jugador",nombre_jugador)

    print("\n")

    print("Se encontraron un total de",counter_req2,"anotaciones desde el punto penal del jugador",nombre_jugador)

    print("\n")

    print("=================TABLE=================")

    print(tabulate(lista_tabulate_req2,keys_req2,tablefmt="fancy_outline"))



def print_req_3(control,high_date,low_date,nombre_equipo):
    """
        Función que imprime la solución del Requerimiento 3 en consola
    """
    
    
    requerimiento_3,contador_local,contador_visitante,size_real_req3,size_real2_req3= controller.req_3(control,high_date,low_date,nombre_equipo)

    lista_tabulate_req3,keys_req3 = tabulate_transformations(requerimiento_3)

    print("================REQ-3================")

    print("Se trabajo con una muestra de",size_real_req3,"equipos con la informacion disponible")

    print("\n")

    print("Se encontraron",size_real2_req3,"partidos del equipo",nombre_equipo)

    print("\n")

    print("Se encontraron",contador_local,"partidos en los que el equipo",nombre_equipo,"jugo como local") 

    print("\n")

    print("Se encontraron",contador_visitante,"partidos en los que el equipo",nombre_equipo,"jugo como visitante") 

    print("\n")

    print("=================TABLE=================")

    print(tabulate(lista_tabulate_req3,keys_req3,tablefmt="fancy_outline"))




def print_req_4(control,nombre_torneo,high_date,low_date):
    """
        Función que imprime la solución del Requerimiento 4 en consola
    """
    requerimiento_4,torneos_total,contador_matches,contador_cities,contador_countries,contador_shootouts = controller.req_4(control,nombre_torneo,high_date,low_date)

    lista_tabulate_req4,keys_req4 = tabulate_transformations(requerimiento_4)

    print("================REQ-4================")

    print("Se trabajo con una muestra de",torneos_total,"torneos con la informacion disponible")

    print("\n")

    print("Se encontraron",contador_matches,"partidos del torneo",nombre_torneo)

    print("\n")

    print("Se encontraron",contador_cities,"ciudades en los que que se jugaron partidos del torneo",nombre_torneo)

    print("\n")

    print("Se encontraron",contador_countries,"paises en los que que se jugaron partidos del torneo",nombre_torneo) 

    print("\n")

    print("Se encontraron",contador_shootouts,"partidos los cuales se definieron por penales en el torneo",nombre_torneo)

    print("\n")

    print("=================TABLE=================")

    print(tabulate(lista_tabulate_req4,keys_req4,tablefmt="fancy_outline"))

    


def print_req_5(control,nombre_jugador,high_date,low_date):
    """
        Función que imprime la solución del Requerimiento 5 en consola
    """

    requerimiento_5,torneos,penalties,autogoles,goles = controller.req_5(control,nombre_jugador,high_date,low_date)

    lista_tabulate_req5,keys_req5 = tabulate_transformations(requerimiento_5)

    print("================REQ-5================")

    print("\n")

    print("Se encontraron",goles,"goles del jugador",nombre_jugador)

    print("\n")

    print("Se encontraron",torneos,"torneos en los cuales participo el jugador",nombre_jugador)

    print("\n")

    print("Se encontraron",penalties,"penalties del jugador",nombre_jugador) 

    print("\n")

    print("Se encontraron",autogoles,"autogoles del jugador",nombre_jugador)

    print("\n")

    print("=================TABLE=================")

    print(tabulate(lista_tabulate_req5,keys_req5,tablefmt="fancy_outline"))



def print_req_6(control,nombre_torneo,anio,n_numero):
    """
        Función que imprime la solución del Requerimiento 6 en consola
    """
    requerimiento_6,ciudades,paises,equipos,partidos,torneos = controller.req_6(control,nombre_torneo,anio,n_numero)

    lista_tabulate_req6,keys_req6 = tabulate_transformation_req6(requerimiento_6)

    print("================REQ-6================")

    print("\n")

    print("Se encontraron",ciudades,"ciudades en las que se jugaron partidos del torneo",nombre_torneo)

    print("\n")

    print("Se encontraron",paises,"paises en las que se jugaron partidos del torneo",nombre_torneo)

    print("\n")

    print("Se encontraron",equipos,"equipos involucrados en el torneo",nombre_torneo) 

    print("\n")

    print("Se encontraron",partidos,"partidos del torneo",nombre_torneo)

    print("\n")

    print("Se encontraron",torneos,"torneos con informacion disponible")

    print(tabulate(lista_tabulate_req6,keys_req6,tablefmt="grid"))



def print_req_7(control,nombre_torneo,n_numero):
    """
        Función que imprime la solución del Requerimiento 7 en consola
    """
    requerimiento_7,tournaments,total_players,matches,goals,penalties,own_goals,players_filtered = controller.req_7(control,nombre_torneo,n_numero)

    lista_tabulate_req7,keys_req7 = tabulate_transformation_req7(requerimiento_7)

    print("================REQ-7================")

    print("\n")

    print("Se encontraron",tournaments,"torneos con informacion disponible")

    print("\n")

    print("Se encontraron",total_players,"jugadores para el torneo",nombre_torneo)

    print("\n")

    print("Se encontraron",matches,"partidos para el torneo",nombre_torneo) 

    print("\n")

    print("Se encontraron",goals,"anotaciones para el torneo torneo",nombre_torneo)

    print("\n")

    print("Se encontraron",penalties,"anotaciones desde el punto penal para el torneo torneo",nombre_torneo)

    print("\n")

    print("Se encontraron",own_goals,"anotaciones de autogol para el torneo torneo",nombre_torneo)

    print("\n")

    print("Se encontraron",players_filtered,"jugadores con un puntaje de",n_numero,"para el torneo",nombre_torneo)

    print("\n")

    print(tabulate(lista_tabulate_req7,keys_req7,tablefmt="grid"))


    


def print_req_8(control):
    """
        Función que imprime la solución del Requerimiento 8 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 8
    pass



            



# Se crea el controlador asociado a la vista


# main del reto
if __name__ == "__main__":
    """
    Menu principal
    """
    working = True
    #ciclo del menu
    while working:
        print_menu()
        inputs = input('Seleccione una opción para continuar\n')
        if int(inputs) == 1:
            size = int((input("Ingrese el tamaño que desea cargar para los archivos: \n 1) 5pct \n 2) 10pct \n 3) 20pct \n 4) 30pct \n 5) 50pct \n 6) 80pct \n 7) small \n 8) large \n")))

            
            if size == 1:
                size = "5pct"
            if size == 2:
                size = "10pct"
            if size == 3:
                size = "20pct"
            if size == 4:
                size = "30pct"
            if size == 5:
                size = "50pct"
            if size == 6:
                size = "80pct"
            if size == 7:
                size = "small"
            if size == 8:
                size = "large"


            choice = int(input("Ingrese la estructura de mapa sobre la cual quiere cargar los mapas de datos: \n 1) PROBING \n 2) CHAINING \n"))


            order = int((input("Ingrese el algoritmo de ordenamiento sobre el cual quiere que se ordenen los datos en las listas: \n 1) MergeSort \n 2) QuickSort \n 3) InsertionSort \n 4) SelectionSort \n")))

            print("Cargando información de los archivos ....\n")


            if order == 1:
                ordenamiento = "MergeSort"
            elif order == 2:
                ordenamiento = "QuickSort"
            elif order == 3:
                ordenamiento = "InsertionSort"
            elif order == 4:
                ordenamiento = "SelectionSort"

            controlador = new_controller(choice)

            results_size,scorers_size,shootouts_size = load_data(controlador,size)

            data_structs_ordered = controller.sort_data(controlador["model"],order)

            print("Se ha cargado la informacion en la estructura de datos ordenados bajo el algoritmo",ordenamiento)

            print("\n")

            print("Se cargaron",results_size,"match results haciendo uso del archivo",size)

            print("\n")

            tabulate_results_lista,keys_results = tabulate_transformations(data_structs_ordered["results"])

            print(tabulate(tabulate_results_lista,headers=keys_results,tablefmt="fancy_outline"))

            print("\n")

            print("Se cargaron",scorers_size,"goalscorers haciendo uso del archivo",size)

            print("\n")

            tabulate_goalscorers_lista,keys_goalscorers = tabulate_transformations(data_structs_ordered["goalscorers"])

            print(tabulate(tabulate_goalscorers_lista,headers=keys_goalscorers,tablefmt="fancy_outline"))
            
            print("\n")

            print("Se cargaron",shootouts_size,"shootouts haciendo uso del archivo",size)

            print("\n")
            
            tabulate_shootouts_lista,keys_shootouts = tabulate_transformations(data_structs_ordered["shootouts"])

            print(tabulate(tabulate_shootouts_lista,headers=keys_shootouts,tablefmt="fancy_outline"))

        elif int(inputs) == 2:

            nombre_equipo_req1 = input("Ingrese el nombre del equipo sobre el cual desea ver los partidos: ")

            condicion = input("Ingrese la condicion sobre la cual desea ver los partidos del equipo ingresado: \n Local \n Visitante \n Indiferente \n")

            n_numero_req1 = int(input("Ingrese el numero de partidos que desea ver del equipo seleccionado: "))

            print_req_1(data_structs_ordered,nombre_equipo_req1,condicion,n_numero_req1)

        elif int(inputs) == 3:
            
            nombre_jugador = input("Ingrese el nombre del jugador sobre el cual desea ver las anotaciones: ")

            n_numero = int(input("Ingrese el numero de anotaciones que desea ver del jugador seleccionado: "))

            print_req_2(controlador,nombre_jugador,n_numero)

        elif int(inputs) == 4:

            nombre_equipo = input("Ingrese el nombre del equipo sobre el cual quiere consultar los resultados: ")

            high_date = input("Ingrese el limite superior de las fechas sobre las cuales desea consultar al equipo usando el formato AAAA-MM-DD: ")

            low_date = input("Ingrese el limite inferior de las fechas sobre las cuales desea consultar al equipo usando el formato AAAA-MM-DD: ")
            
            print_req_3(controlador,high_date,low_date,nombre_equipo)

        elif int(inputs) == 5:

            nombre_torneo = input("Ingrese el nombre del torneo sobre el cual quiere consultar los resultados: ")

            high_date = input("Ingrese el limite superior de las fechas sobre las cuales desea consultar al torneo usando el formato AAAA-MM-DD: ")

            low_date = input("Ingrese el limite inferior de las fechas sobre las cuales desea consultar al torneo usando el formato AAAA-MM-DD: ")

            print_req_4(controlador,nombre_torneo,high_date,low_date)

        elif int(inputs) == 6:

            nombre_jugador = input("Ingrese el nombre del jugador sobre el cual quiere consultar las anotaciones: ")

            high_date = input("Ingrese el limite superior de las fechas sobre las cuales desea consultar al jugador usando el formato AAAA-MM-DD: ")

            low_date = input("Ingrese el limite inferior de las fechas sobre las cuales desea consultar al jugador usando el formato AAAA-MM-DD: ")

            print_req_5(data_structs_ordered,nombre_jugador,high_date,low_date)

        elif int(inputs) == 7:
            
            nombre_torneo = input("Ingrese el nombre del torneo sobre el cual quiere consultar los resultados: ")

            anio = input("Ingrese el año sobre el cual quisiera consultar los equipos del torneo: ")

            n_numero = int(input("Ingrese el numero de equipos que desea ver del torneo seleccionado: "))

            print_req_6(controlador,nombre_torneo,anio,n_numero)

        elif int(inputs) == 8:

            nombre_torneo = input("Ingrese el nombre del torneo sobre el cual quiere consultar los jugadores: ")

            n_numero = int(input("Ingrese el puntaje (N) de los jugadores dentro del torneo: "))

            print_req_7(controlador,nombre_torneo,n_numero)

        elif int(inputs) == 9:
            print_req_8(controlador)

        elif int(inputs) == 0:
            working = False
            print("\nGracias por utilizar el programa") 
        else:
            print("Opción errónea, vuelva a elegir.\n")
    sys.exit(0)
