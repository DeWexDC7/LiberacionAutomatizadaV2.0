import Inventario_naps as inv_naps
import Liberacion as lib_clouster
import os
import time
import sys
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

def animacion_bienvenida():
    """Muestra una animación de bienvenida al iniciar el programa."""
    limpiar_pantalla()
    
    # Caracteres para la animación
    frames = ["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"]
    
    # Mensajes para mostrar durante la carga
    mensajes = [
        "Iniciando sistema...",
        "Cargando módulos...",
        "Verificando componentes...",
        "Preparando interfaz...",
        "¡Bienvenido al Sistema de Gestión Automatizada!"
    ]
    
    # Animación de carga
    for i, mensaje in enumerate(mensajes):
        for frame in frames:
            sys.stdout.write('\r')
            if i < len(mensajes) - 1:
                sys.stdout.write(Fore.CYAN + f"{frame} {mensaje}")
            else:
                sys.stdout.write(Fore.GREEN + Style.BRIGHT + f"{mensaje}")
            sys.stdout.flush()
            time.sleep(0.1)
    
    print("\n")
    time.sleep(1)

def mostrar_menu():
    """Muestra el menú de opciones con formato mejorado."""
    print(Fore.WHITE + Style.BRIGHT + "  MENÚ PRINCIPAL:")
    print(Fore.CYAN + "  " + "-" * 40)
    print(Fore.GREEN + "  1. " + Fore.WHITE + "Liberación de Clouster")
    print(Fore.GREEN + "  2. " + Fore.WHITE + "Actualización de NAPs")
    print(Fore.GREEN + "  3. " + Fore.WHITE + "Ayuda")
    print(Fore.RED + "  4. " + Fore.WHITE + "Salir")
    print(Fore.CYAN + "  " + "-" * 40)
    print(Fore.YELLOW + "  Presione 'h' para ayuda en cualquier momento")

def mostrar_ayuda():
    """Muestra información de ayuda sobre el sistema."""
    limpiar_pantalla()
    print(Fore.CYAN + "=" * 50)
    print(Fore.YELLOW + Style.BRIGHT + "           SISTEMA DE AYUDA")
    print(Fore.CYAN + "=" * 50 + "\n")
    
    print(Fore.WHITE + Style.BRIGHT + "DESCRIPCIÓN GENERAL:")
    print(Fore.WHITE + "  Este sistema permite gestionar de forma automatizada")
    print(Fore.WHITE + "  diferentes procesos relacionados con la liberación")
    print(Fore.WHITE + "  de Clouster y actualización de NAPs.\n")
    
    print(Fore.WHITE + Style.BRIGHT + "OPCIONES DISPONIBLES:")
    print(Fore.GREEN + "  1. Liberación de Clouster:" + Fore.WHITE + " Facilita el proceso de")
    print(Fore.WHITE + "     liberación automatizada de Clouster.")
    print(Fore.GREEN + "  2. Actualización de NAPs:" + Fore.WHITE + " Permite actualizar y")
    print(Fore.WHITE + "     gestionar el inventario de NAPs en el sistema.")
    print(Fore.GREEN + "  3. Ayuda:" + Fore.WHITE + " Muestra esta pantalla de ayuda.")
    print(Fore.GREEN + "  4. Salir:" + Fore.WHITE + " Cierra el programa.\n")
    
    print(Fore.WHITE + Style.BRIGHT + "ATAJOS DE TECLADO:")
    print(Fore.WHITE + "  • Presione 'h' en cualquier momento para mostrar la ayuda")
    print(Fore.WHITE + "  • Presione 'q' para volver al menú principal desde los módulos")
    print(Fore.WHITE + "  • Presione 'x' para salir del programa\n")
    
    input(Fore.CYAN + "Presione ENTER para volver al menú principal...")

def eleccion():
    """Controla la elección del usuario con una interfaz mejorada y manejo de errores."""
    mostrar_encabezado()
    mostrar_menu()
    
    while True:
        try:
            opcion = input(Fore.YELLOW + "\n  Ingrese su opción: " + Fore.WHITE).lower()
            
            # Verificar atajos globales
            if opcion in ['h', 'help', '?']:
                mostrar_ayuda()
                mostrar_encabezado()
                mostrar_menu()
                continue
            elif opcion in ['x', 'exit', 'quit']:
                confirmar_salida()
                continue
            
            # Procesar opciones numéricas
            if opcion == "1":
                limpiar_pantalla()
                print(Fore.CYAN + "\nIniciando módulo de Liberación de Clouster...\n")
                time.sleep(1)
                try:
                    lib_clouster.main()
                except Exception as e:
                    print(Fore.RED + f"\nError en el módulo de Liberación: {str(e)}")
                    time.sleep(2)
                volver_al_menu()
            elif opcion == "2":
                limpiar_pantalla()
                print(Fore.CYAN + "\nIniciando módulo de Actualización de NAPs...\n")
                time.sleep(1)
                try:
                    inv_naps.main()
                except Exception as e:
                    print(Fore.RED + f"\nError en el módulo de NAPs: {str(e)}")
                    time.sleep(2)
                volver_al_menu()
            elif opcion == "3":
                mostrar_ayuda()
                mostrar_encabezado()
                mostrar_menu()
            elif opcion == "4":
                confirmar_salida()
            else:
                print(Fore.RED + "\n¡Opción inválida! Por favor, seleccione una opción válida.")
                time.sleep(1)
        except KeyboardInterrupt:
            print(Fore.YELLOW + "\n\nOperación interrumpida por el usuario.")
            time.sleep(1)
            mostrar_encabezado()
            mostrar_menu()

def confirmar_salida():
    """Solicita confirmación antes de salir del programa."""
    print(Fore.YELLOW + "\n¿Está seguro que desea salir del programa? (s/n): ", end="")
    respuesta = input().lower()
    if respuesta in ['s', 'si', 'yes', 'y']:
        limpiar_pantalla()
        print(Fore.YELLOW + "\nCerrando el programa. ¡Hasta pronto!")
        
        # Animación de cierre
        for i in range(5, 0, -1):
            sys.stdout.write('\r')
            sys.stdout.write(Fore.CYAN + f"El programa se cerrará en {i} segundos...")
            sys.stdout.flush()
            time.sleep(0.5)
        
        sys.exit(0)
    else:
        mostrar_encabezado()
        mostrar_menu()

def volver_al_menu():
    """Función para volver al menú principal después de completar una tarea."""
    print("")
    input(Fore.CYAN + "Presione ENTER para volver al menú principal...")
    eleccion()
    
def main():
    animacion_bienvenida()
    eleccion()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        limpiar_pantalla()
        print(Fore.YELLOW + "\nPrograma interrumpido por el usuario. ¡Hasta pronto!")
        sys.exit(0)
    except Exception as e:
        limpiar_pantalla()
        print(Fore.RED + f"\nError inesperado: {str(e)}")
        print(Fore.YELLOW + "El programa se cerrará en 3 segundos...")
        time.sleep(3)
        sys.exit(1)