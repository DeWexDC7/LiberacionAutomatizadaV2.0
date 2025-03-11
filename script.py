import Inventario_naps as inv_naps
import Liberacion as lib_clouster
import os
import time
from colorama import Fore, Back, Style, init

# Inicializar colorama
init(autoreset=True)

def limpiar_pantalla():
    """Limpia la pantalla de la consola."""
    os.system('cls' if os.name == 'nt' else 'clear')

def mostrar_encabezado():
    """Muestra un encabezado atractivo para el programa."""
    limpiar_pantalla()
    print(Fore.CYAN + "=" * 50)
    print(Fore.YELLOW + Style.BRIGHT + "       SISTEMA DE GESTIÓN AUTOMATIZADA")
    print(Fore.CYAN + "=" * 50)
    print("")

def mostrar_menu():
    """Muestra el menú de opciones con formato mejorado."""
    print(Fore.WHITE + Style.BRIGHT + "  MENÚ PRINCIPAL:")
    print(Fore.CYAN + "  " + "-" * 40)
    print(Fore.GREEN + "  1. " + Fore.WHITE + "Liberación de Clouster")
    print(Fore.GREEN + "  2. " + Fore.WHITE + "Actualización de NAPs")
    print(Fore.RED + "  3. " + Fore.WHITE + "Salir")
    print(Fore.CYAN + "  " + "-" * 40)

def eleccion():
    """Controla la elección del usuario con una interfaz mejorada."""
    mostrar_encabezado()
    mostrar_menu()
    
    opcion = input(Fore.YELLOW + "\n  Ingrese su opción [1-3]: " + Fore.WHITE)
    
    if opcion == "1":
        limpiar_pantalla()
        print(Fore.CYAN + "\nIniciando módulo de Liberación de Clouster...\n")
        time.sleep(1)  # Breve pausa para mejorar la experiencia
        lib_clouster.main()
        volver_al_menu()
    elif opcion == "2":
        limpiar_pantalla()
        print(Fore.CYAN + "\nIniciando módulo de Actualización de NAPs...\n")
        time.sleep(1)  # Breve pausa para mejorar la experiencia
        inv_naps.main()
        volver_al_menu()
    elif opcion == "3":
        limpiar_pantalla()
        print(Fore.YELLOW + "\nCerrando el programa. ¡Hasta pronto!")
        time.sleep(1)
        exit()
    else:
        print(Fore.RED + "\n¡Opción inválida! Por favor, seleccione una opción válida.")
        time.sleep(2)
        eleccion()  # Volver a mostrar el menú

def volver_al_menu():
    """Función para volver al menú principal después de completar una tarea."""
    print("")
    input(Fore.CYAN + "Presione ENTER para volver al menú principal...")
    eleccion()
    
def main():
    eleccion()

if __name__ == "__main__":
    main()