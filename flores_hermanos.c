/*
 * TRABAJO DE UNIDAD I - Compra concurrente de pasajes de bus
 * Caso: Flores Hermanos, bus interprovincial de dos pisos.
 *
 * Compilar (Linux o terminal MSYS2 UCRT64 con GCC y pthreads):
 *   gcc -std=c11 -O2 -Wall -Wextra -Wpedantic -pthread flores_hermanos.c -o flores
 * Ejecutar:
 *   ./flores
 *   ./flores --piso1 2 --piso2 4 --pasajeros 12 --segundos 4 --semilla 42
 *   ./flores --ayuda
 *
 * REQUISITOS DE LA CONSIGNA
 * - Lenguaje C y un hilo pthread por pasajero, con identificadores 1..p_num.
 * - Un pthread_mutex_t por asiento, con identificadores globales 1..s_num.
 * - Compras aleatorias y concurrentes; impresion de cada compra y resumen.
 * - Variables globales tiempo_inicio, tiempo_simulacion, s_num y p_num.
 * - Ciudad de origen, destino y asientos distribuidos en dos pisos.
 *
 * MODELO ACADEMICO
 * 60 asientos (12 + 48), 90 pasajeros, Lima -> Arequipa y 10 segundos son
 * supuestos configurables; no son datos oficiales de la empresa. Cada
 * pasajero compra como maximo un boleto. El tiempo de compra esta comprimido
 * para observar el proceso en clase. No se procesan pagos reales.
 *
 * SINCRONIZACION
 * El mutex de un asiento permanece tomado desde la consulta de disponibilidad
 * hasta confirmar o cancelar el pago. Se usa trylock: si otro pasajero lo
 * esta pagando, el hilo espera brevemente y vuelve a elegir. Los asientos
 * distintos permiten pagos en paralelo. No se usa un bloqueo global para
 * ejecutar toda la compra. Los mutex de estadisticas y consola solo protegen
 * operaciones breves; nunca se duerme sosteniendolos.
 *
 * Orden de bloqueos: asiento -> estadisticas O asiento -> consola.
 * No se adquiere un asiento mientras se retiene otro mutex; no hay ciclos.
 * El estado aleatorio y las estadisticas de cada pasajero pertenecen a su
 * hilo. main solo los lee despues de pthread_join. La configuracion queda
 * inmutable antes de iniciar; el reloj se publica mediante una condicion.
 *
 * COMPLEJIDAD
 * Sea S la cantidad de asientos, P la de pasajeros y A el total de intentos.
 * Inicializacion y resumen: O(S + P). Cada intento: O(1), sin contar esperas
 * de E/S y planificacion. Trabajo total: O(S + P + A). Memoria: O(S + P),
 * ademas de las pilas de P hilos. Cada intento fallido incluye una pausa,
 * por lo que no hay espera activa. No se garantiza equidad entre pasajeros.
 */

#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Variables globales exigidas: tiempos expresados en segundos. */
double tiempo_inicio = 0.0;       /* Referencia del reloj monotonico. */
double tiempo_simulacion = 10.0;  /* Duracion de la ventana de ventas. */
int s_num = 60;
int p_num = 90;

typedef struct {
    int id;
    int piso;
    int numero_en_piso;
    pthread_mutex_t mutex;
    int comprador_id;            /* 0: libre; otro valor: ID del comprador. */
    int ventas;                  /* Debe ser siempre 0 o 1. */
    double instante_compra;      /* Segundos transcurridos al confirmar. */
} Asiento;

typedef enum { FIN_TIEMPO, SIN_CUPO, COMPRA_EXITOSA } Resultado;

typedef struct {
    int id;
    uint32_t rng;                /* Generador propio: no comparte rand(). */
    int piso_preferido;
    int llegada_ms;
    int asiento_comprado;
    unsigned long intentos;
    unsigned long conflictos;    /* Mutex del asiento tomado por otro hilo. */
    unsigned long ya_vendidos;
    unsigned long pagos_fallidos;
    unsigned long pagos_vencidos;
    Resultado resultado;
} Pasajero;

static Asiento *asientos = NULL;
static Pasajero *pasajeros = NULL;
static int asientos_piso1 = 12;
static int asientos_piso2 = 48;
static int fallo_pago_pct = 8;
static int ventana_llegadas_ms = -1;
static uint32_t semilla = 0;
static const char *origen = "Lima";
static const char *destino = "Arequipa";

static pthread_mutex_t mutex_inicio = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t condicion_inicio = PTHREAD_COND_INITIALIZER;
static int inicio_publicado = 0;
static int cancelar_inicio = 0;
static pthread_mutex_t mutex_consola = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_estadisticas = PTHREAD_MUTEX_INITIALIZER;
static int vendidos = 0;
static int activos = 0;
static int max_activos = 0;
static int pagos_activos = 0;
static int max_pagos_activos = 0;

/* Los errores de pthreads son codigos devueltos, no necesariamente errno. */
static void comprobar_pthread(int codigo, const char *operacion)
{
    if (codigo != 0) {
        fprintf(stderr, "ERROR en %s: %s (codigo %d)\n",
                operacion, strerror(codigo), codigo);
        exit(EXIT_FAILURE);
    }
}

static double reloj_segundos(void)
{
    struct timespec ahora;
    if (clock_gettime(CLOCK_MONOTONIC, &ahora) != 0) {
        perror("clock_gettime");
        exit(EXIT_FAILURE);
    }
    return (double)ahora.tv_sec + (double)ahora.tv_nsec / 1000000000.0;
}

/* El reloj monotonico evita cambios por ajustes de la hora del sistema. */
static int hay_tiempo(void)
{
    return reloj_segundos() < tiempo_inicio + tiempo_simulacion;
}

static void esperar_hasta(double objetivo)
{
    for (;;) {
        double restante = objetivo - reloj_segundos();
        struct timespec pausa;
        if (restante <= 0.0) {
            return;
        }
        pausa.tv_sec = (time_t)restante;
        pausa.tv_nsec = (long)((restante - (double)pausa.tv_sec) * 1e9);
        if (pausa.tv_sec == 0 && pausa.tv_nsec == 0) {
            pausa.tv_nsec = 1;
        }
        if (nanosleep(&pausa, NULL) != 0 && errno != EINTR) {
            perror("nanosleep");
            exit(EXIT_FAILURE);
        }
    }
}

/* Una espera de compra o reintento nunca se programa mas alla del cierre. */
static void pausar_ms(int milisegundos)
{
    double objetivo = reloj_segundos() + (double)milisegundos / 1000.0;
    double fin = tiempo_inicio + tiempo_simulacion;
    esperar_hasta(objetivo < fin ? objetivo : fin);
}

/* xorshift32: aleatoriedad de simulacion; no se usa para criptografia. */
static uint32_t aleatorio(Pasajero *p)
{
    uint32_t x = p->rng;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    p->rng = x;
    return x;
}

static int entre(Pasajero *p, int minimo, int maximo)
{
    return minimo + (int)(aleatorio(p) % (uint32_t)(maximo - minimo + 1));
}

/* Solo se serializa la impresion de cada linea, no el proceso de compra. */
static void evento(int id_pasajero, const char *formato, ...)
{
    va_list argumentos;
    comprobar_pthread(pthread_mutex_lock(&mutex_consola), "lock consola");
    printf("[%9.6f s] P%04d | ", reloj_segundos() - tiempo_inicio, id_pasajero);
    va_start(argumentos, formato);
    vprintf(formato, argumentos);
    va_end(argumentos);
    putchar('\n');
    comprobar_pthread(pthread_mutex_unlock(&mutex_consola), "unlock consola");
}

static int bus_lleno(void)
{
    int lleno;
    comprobar_pthread(pthread_mutex_lock(&mutex_estadisticas), "lock estadisticas");
    lleno = vendidos == s_num;
    comprobar_pthread(pthread_mutex_unlock(&mutex_estadisticas), "unlock estadisticas");
    return lleno;
}

static void cambiar_activos(int cambio)
{
    comprobar_pthread(pthread_mutex_lock(&mutex_estadisticas), "lock estadisticas");
    activos += cambio;
    if (activos > max_activos) {
        max_activos = activos;
    }
    comprobar_pthread(pthread_mutex_unlock(&mutex_estadisticas), "unlock estadisticas");
}

static void cambiar_pagos(int cambio, int nueva_venta)
{
    comprobar_pthread(pthread_mutex_lock(&mutex_estadisticas), "lock estadisticas");
    pagos_activos += cambio;
    vendidos += nueva_venta;
    if (pagos_activos > max_pagos_activos) {
        max_pagos_activos = pagos_activos;
    }
    comprobar_pthread(pthread_mutex_unlock(&mutex_estadisticas), "unlock estadisticas");
}

static int elegir_asiento(Pasajero *p)
{
    /* 70% de intentos en el piso preferido; 30% en cualquier asiento. */
    if (entre(p, 1, 100) <= 70) {
        if (p->piso_preferido == 1) {
            return entre(p, 0, asientos_piso1 - 1);
        }
        return entre(p, asientos_piso1, s_num - 1);
    }
    return entre(p, 0, s_num - 1);
}

/* Se invoca con a->mutex tomado y siempre lo devuelve aun tomado. */
static int procesar_pago(Pasajero *p, Asiento *a)
{
    int exito = 0;
    double instante;
    cambiar_pagos(1, 0);
    evento(p->id, "RESERVA temporal A%03d, piso %d; procesando pago.", a->id, a->piso);
    pausar_ms(entre(p, 120, 450));
    instante = reloj_segundos() - tiempo_inicio;

    if (instante >= tiempo_simulacion) {
        ++p->pagos_vencidos;
        evento(p->id, "CANCELADO A%03d: termino el tiempo; asiento liberado.", a->id);
    } else if (entre(p, 1, 100) <= fallo_pago_pct) {
        ++p->pagos_fallidos;
        evento(p->id, "PAGO FALLIDO A%03d: asiento liberado; reintentara.", a->id);
    } else {
        /* SECCION CRITICA: comprobar y vender usan el MISMO mutex. */
        a->comprador_id = p->id;
        a->instante_compra = instante;
        ++a->ventas;
        p->asiento_comprado = a->id;
        p->resultado = COMPRA_EXITOSA;
        exito = 1;
    }

    cambiar_pagos(-1, exito);
    if (exito) {
        evento(p->id, "COMPRA CONFIRMADA A%03d | piso %d | %s -> %s | t=%.6f s",
               a->id, a->piso, origen, destino, a->instante_compra);
    }
    return exito;
}

static void *simular_pasajero(void *argumento)
{
    Pasajero *p = argumento;
    comprobar_pthread(pthread_mutex_lock(&mutex_inicio), "lock inicio");
    while (!inicio_publicado && !cancelar_inicio) {
        comprobar_pthread(pthread_cond_wait(&condicion_inicio, &mutex_inicio), "wait inicio");
    }
    if (cancelar_inicio) {
        comprobar_pthread(pthread_mutex_unlock(&mutex_inicio), "unlock inicio");
        return NULL;
    }
    comprobar_pthread(pthread_mutex_unlock(&mutex_inicio), "unlock inicio");

    esperar_hasta(tiempo_inicio + (double)p->llegada_ms / 1000.0);
    if (!hay_tiempo()) {
        evento(p->id, "SIN COMPRA: llego al cierre de la simulacion.");
        return NULL;
    }
    cambiar_activos(1);
    evento(p->id, "LLEGA; prefiere piso %d.", p->piso_preferido);

    while (hay_tiempo()) {
        Asiento *a;
        int codigo;
        int exito = 0;
        if (bus_lleno()) {
            p->resultado = SIN_CUPO;
            break;
        }
        a = &asientos[elegir_asiento(p)];
        ++p->intentos;
        codigo = pthread_mutex_trylock(&a->mutex);
        if (codigo == EBUSY) {
            ++p->conflictos;
            evento(p->id, "CONFLICTO A%03d: otro pasajero lo esta procesando.", a->id);
        } else {
            comprobar_pthread(codigo, "trylock asiento");
            if (!hay_tiempo()) {
                comprobar_pthread(pthread_mutex_unlock(&a->mutex), "unlock asiento");
                break;
            }
            if (a->comprador_id != 0) {
                ++p->ya_vendidos;
                evento(p->id, "A%03d YA VENDIDO; elegira otro asiento.", a->id);
            } else {
                exito = procesar_pago(p, a);
            }
            comprobar_pthread(pthread_mutex_unlock(&a->mutex), "unlock asiento");
        }
        if (exito) {
            break; /* Un pasajero no puede comprar un segundo asiento. */
        }
        pausar_ms(entre(p, 25, 90));
    }

    cambiar_activos(-1);
    if (p->resultado != COMPRA_EXITOSA) {
        evento(p->id, "SIN COMPRA: %s.",
               p->resultado == SIN_CUPO ? "bus sin cupos" : "fin del tiempo");
    }
    return NULL;
}

static void ayuda(const char *programa)
{
    printf("Uso: %s [opcion valor ...]\n"
           "  --piso1 N        Asientos del primer piso: 1..80 (12).\n"
           "  --piso2 N        Asientos del segundo piso: 1..80 (48).\n"
           "  --pasajeros N    Hilos de pasajeros: 1..1000 (90).\n"
           "  --segundos T     Duracion: 0.1..3600 segundos (10).\n"
           "  --semilla N      Semilla: 0..4294967295 (hora actual).\n"
           "  --origen TEXTO   Ciudad de origen (Lima).\n"
           "  --destino TEXTO  Ciudad de destino (Arequipa).\n"
           "  --fallo-pago N   Probabilidad de pago fallido: 0..100 %% (8).\n"
           "  --llegada-ms N   Llegadas entre 0 y N ms (60 %% de la duracion).\n"
           "  --ayuda          Muestra esta ayuda.\n"
           "Ejemplo: %s --piso1 2 --piso2 4 --pasajeros 12 --segundos 4 --semilla 42\n"
           "La semilla fija las secuencias aleatorias de cada hilo; el orden\n"
           "de ejecucion y los ganadores dependen del planificador del SO.\n",
           programa, programa);
}

static void argumento_invalido(const char *opcion, const char *valor)
{
    fprintf(stderr, "Argumento invalido: %s %s. Consulte --ayuda.\n", opcion, valor);
    exit(EXIT_FAILURE);
}

static long long entero(const char *opcion, const char *valor, long long min, long long max)
{
    char *fin;
    long long numero;
    errno = 0;
    numero = strtoll(valor, &fin, 10);
    if (errno != 0 || fin == valor || *fin != '\0' || numero < min || numero > max) {
        argumento_invalido(opcion, valor);
    }
    return numero;
}

static void configurar(int argc, char **argv)
{
    int i;
    semilla = (uint32_t)time(NULL);
    for (i = 1; i < argc; ++i) {
        const char *opcion = argv[i];
        const char *valor;
        if (strcmp(opcion, "--ayuda") == 0) {
            ayuda(argv[0]);
            exit(EXIT_SUCCESS);
        }
        if (i + 1 == argc) {
            argumento_invalido(opcion, "(falta el valor)");
        }
        valor = argv[++i];
        if (strcmp(opcion, "--piso1") == 0) {
            asientos_piso1 = (int)entero(opcion, valor, 1, 80);
        } else if (strcmp(opcion, "--piso2") == 0) {
            asientos_piso2 = (int)entero(opcion, valor, 1, 80);
        } else if (strcmp(opcion, "--pasajeros") == 0) {
            p_num = (int)entero(opcion, valor, 1, 1000);
        } else if (strcmp(opcion, "--semilla") == 0) {
            semilla = (uint32_t)entero(opcion, valor, 0, 4294967295LL);
        } else if (strcmp(opcion, "--fallo-pago") == 0) {
            fallo_pago_pct = (int)entero(opcion, valor, 0, 100);
        } else if (strcmp(opcion, "--llegada-ms") == 0) {
            ventana_llegadas_ms = (int)entero(opcion, valor, 0, 3600000);
        } else if (strcmp(opcion, "--segundos") == 0) {
            char *fin;
            errno = 0;
            tiempo_simulacion = strtod(valor, &fin);
            if (errno != 0 || fin == valor || *fin != '\0' ||
                !(tiempo_simulacion >= 0.1 && tiempo_simulacion <= 3600.0)) {
                argumento_invalido(opcion, valor);
            }
        } else if (strcmp(opcion, "--origen") == 0 || strcmp(opcion, "--destino") == 0) {
            size_t longitud = strlen(valor);
            size_t j;
            if (longitud == 0 || longitud > 80) {
                argumento_invalido(opcion, valor);
            }
            for (j = 0; j < longitud; ++j) {
                if ((unsigned char)valor[j] < 32 || (unsigned char)valor[j] == 127) {
                    argumento_invalido(opcion, valor);
                }
            }
            if (strcmp(opcion, "--origen") == 0) origen = valor;
            else destino = valor;
        } else {
            argumento_invalido(opcion, valor);
        }
    }
    s_num = asientos_piso1 + asientos_piso2;
    if (ventana_llegadas_ms < 0) {
        ventana_llegadas_ms = (int)(tiempo_simulacion * 600.0);
    }
    if ((double)ventana_llegadas_ms > tiempo_simulacion * 1000.0) {
        fprintf(stderr, "La ventana de llegadas no puede superar la duracion.\n");
        exit(EXIT_FAILURE);
    }
    if (strcmp(origen, destino) == 0) {
        fprintf(stderr, "Origen y destino deben ser distintos.\n");
        exit(EXIT_FAILURE);
    }
}

/* Se ejecuta exclusivamente despues de unir TODOS los hilos. */
static int imprimir_y_validar_resumen(void)
{
    int i, piso;
    int total_asientos = 0, total_pasajeros = 0;
    int sin_cupo = 0, sin_tiempo = 0;
    int correcto = 1;
    unsigned long intentos = 0, conflictos = 0, ocupados = 0;
    unsigned long fallos = 0, vencidos = 0;

    puts("\n================ RESUMEN FINAL ================");
    printf("Flores Hermanos | %s -> %s | dos pisos\n", origen, destino);
    puts("MAPA: [asiento:pasajero] o [asiento:LIBRE]. Numeracion unica en ambos pisos.");
    for (piso = 1; piso <= 2; ++piso) {
        int columnas = 0;
        printf("PISO %d\n", piso);
        for (i = 0; i < s_num; ++i) {
            const Asiento *a = &asientos[i];
            if (a->piso != piso) continue;
            if (a->comprador_id == 0) printf("[A%03d:LIBRE] ", a->id);
            else printf("[A%03d:P%04d] ", a->id, a->comprador_id);
            if (++columnas % 4 == 0) putchar('\n');
        }
        if (columnas % 4 != 0) putchar('\n');
    }

    puts("\nASIENTOS COMPRADOS (tabla de boletos)");
    puts("Asiento | Piso | Nro. en piso | Pasajero | Confirmacion (s)");
    for (i = 0; i < s_num; ++i) {
        const Asiento *a = &asientos[i];
        if (a->comprador_id != 0) {
            ++total_asientos;
            printf("A%03d    | %d    | %3d          | P%04d    | %.6f\n",
                   a->id, a->piso, a->numero_en_piso, a->comprador_id, a->instante_compra);
            if (a->ventas != 1 || a->comprador_id < 1 || a->comprador_id > p_num ||
                a->instante_compra < 0.0 || a->instante_compra >= tiempo_simulacion) {
                correcto = 0;
            } else if (pasajeros[a->comprador_id - 1].asiento_comprado != a->id) {
                correcto = 0;
            }
        } else if (a->ventas != 0) {
            correcto = 0;
        }
    }
    if (total_asientos == 0) puts("No se confirmaron compras.");

    for (i = 0; i < p_num; ++i) {
        const Pasajero *p = &pasajeros[i];
        intentos += p->intentos;
        conflictos += p->conflictos;
        ocupados += p->ya_vendidos;
        fallos += p->pagos_fallidos;
        vencidos += p->pagos_vencidos;
        if (p->resultado == COMPRA_EXITOSA) {
            ++total_pasajeros;
            if (p->asiento_comprado < 1 || p->asiento_comprado > s_num ||
                asientos[p->asiento_comprado - 1].comprador_id != p->id) correcto = 0;
        } else {
            if (p->asiento_comprado != 0) correcto = 0;
            if (p->resultado == SIN_CUPO) ++sin_cupo;
            else ++sin_tiempo;
        }
    }

    printf("\nAsientos totales: %d | vendidos: %d | libres: %d\n",
           s_num, total_asientos, s_num - total_asientos);
    printf("Pasajeros: %d | con boleto: %d | sin cupo: %d | por tiempo: %d\n",
           p_num, total_pasajeros, sin_cupo, sin_tiempo);
    printf("Intentos: %lu | conflictos de mutex: %lu | ya vendidos: %lu\n",
           intentos, conflictos, ocupados);
    printf("Pagos fallidos: %lu | pagos cancelados al cierre: %lu\n", fallos, vencidos);
    printf("Maximo de pasajeros activos a la vez: %d\n", max_activos);
    printf("Maximo de pagos simultaneos en asientos distintos: %d\n", max_pagos_activos);
    printf("Ocupacion final: %.2f %%\n", 100.0 * (double)total_asientos / (double)s_num);
    printf("Ventana de ventas: %.3f s | tiempo hasta reunir los hilos: %.6f s\n",
           tiempo_simulacion, reloj_segundos() - tiempo_inicio);

    if (total_asientos != total_pasajeros || total_asientos != vendidos ||
        total_asientos > s_num || total_pasajeros + sin_cupo + sin_tiempo != p_num ||
        activos != 0 || pagos_activos != 0 || max_pagos_activos > s_num ||
        max_pagos_activos > p_num) correcto = 0;
    printf("VERIFICACION: %s\n", correcto ? "OK" : "ERROR");
    if (correcto) {
        puts("Sin sobreventa; maximo un boleto por pasajero; compras dentro del plazo;");
        puts("conteos y relaciones asiento-pasajero consistentes; sin pagos pendientes.");
    }
    return correcto;
}

int main(int argc, char **argv)
{
    pthread_t *hilos;
    int i;
    int creados = 0;
    int mutex_creados = 0;
    int correcto = 0;
    configurar(argc, argv);
    /* Salida por lineas completas. El tamano BUFSIZ es necesario: en Windows
     * (UCRT/MSYS2) un tamano 0 con _IOLBF es invalido y puede abortar. */
    if (setvbuf(stdout, NULL, _IOLBF, BUFSIZ) != 0) {
        perror("setvbuf");
        return EXIT_FAILURE;
    }
    asientos = calloc((size_t)s_num, sizeof(*asientos));
    pasajeros = calloc((size_t)p_num, sizeof(*pasajeros));
    hilos = calloc((size_t)p_num, sizeof(*hilos));
    if (asientos == NULL || pasajeros == NULL || hilos == NULL) {
        fprintf(stderr, "No hay memoria suficiente para la simulacion.\n");
        goto liberar;
    }

    for (i = 0; i < s_num; ++i) {
        int codigo;
        asientos[i].id = i + 1;
        asientos[i].piso = i < asientos_piso1 ? 1 : 2;
        asientos[i].numero_en_piso = i < asientos_piso1 ? i + 1 : i - asientos_piso1 + 1;
        codigo = pthread_mutex_init(&asientos[i].mutex, NULL);
        if (codigo != 0) {
            fprintf(stderr, "No se pudo crear el mutex del asiento %d: %s\n", i + 1, strerror(codigo));
            goto liberar;
        }
        ++mutex_creados;
    }
    for (i = 0; i < p_num; ++i) {
        int codigo;
        Pasajero *p = &pasajeros[i];
        p->id = i + 1;
        p->rng = semilla ^ (UINT32_C(2654435769) * (uint32_t)p->id);
        if (p->rng == 0) p->rng = UINT32_C(2463534242);
        p->piso_preferido = entre(p, 1, 2);
        p->llegada_ms = entre(p, 0, ventana_llegadas_ms);
        p->resultado = FIN_TIEMPO;
        codigo = pthread_create(&hilos[i], NULL, simular_pasajero, p);
        if (codigo != 0) {
            fprintf(stderr, "No se pudo crear el hilo del pasajero %d: %s\n", p->id, strerror(codigo));
            comprobar_pthread(pthread_mutex_lock(&mutex_inicio), "lock inicio");
            cancelar_inicio = 1;
            comprobar_pthread(pthread_cond_broadcast(&condicion_inicio), "broadcast cancelacion");
            comprobar_pthread(pthread_mutex_unlock(&mutex_inicio), "unlock inicio");
            for (i = 0; i < creados; ++i) {
                comprobar_pthread(pthread_join(hilos[i], NULL), "join cancelacion");
            }
            goto liberar;
        }
        ++creados;
    }

    puts("SIMULACION DE VENTA DE PASAJES - FLORES HERMANOS");
    printf("Ruta: %s -> %s | bus interprovincial de DOS PISOS\n", origen, destino);
    printf("Asientos: %d (piso 1: %d; piso 2: %d) | pasajeros/hilos: %d\n",
           s_num, asientos_piso1, asientos_piso2, p_num);
    printf("Duracion: %.3f s | semilla: %lu | fallo de pago: %d %%\n",
           tiempo_simulacion, (unsigned long)semilla, fallo_pago_pct);
    printf("Llegadas aleatorias: 0..%d ms | pagos: 120..450 ms\n", ventana_llegadas_ms);
    puts("Datos y tiempos hipoteticos para fines academicos. Un boleto por pasajero.");
    puts("Cada asiento tiene su mutex; RESERVA no significa COMPRA CONFIRMADA.\n");

    comprobar_pthread(pthread_mutex_lock(&mutex_inicio), "lock inicio");
    tiempo_inicio = reloj_segundos();
    inicio_publicado = 1;
    comprobar_pthread(pthread_cond_broadcast(&condicion_inicio), "broadcast inicio");
    comprobar_pthread(pthread_mutex_unlock(&mutex_inicio), "unlock inicio");

    /* Se conserva toda la ventana solicitada aunque el bus se llene antes. */
    esperar_hasta(tiempo_inicio + tiempo_simulacion);
    for (i = 0; i < creados; ++i) {
        comprobar_pthread(pthread_join(hilos[i], NULL), "join pasajero");
    }
    correcto = imprimir_y_validar_resumen();

liberar:
    for (i = 0; i < mutex_creados; ++i) {
        comprobar_pthread(pthread_mutex_destroy(&asientos[i].mutex), "destroy asiento");
    }
    comprobar_pthread(pthread_cond_destroy(&condicion_inicio), "destroy condicion");
    comprobar_pthread(pthread_mutex_destroy(&mutex_inicio), "destroy inicio");
    comprobar_pthread(pthread_mutex_destroy(&mutex_consola), "destroy consola");
    comprobar_pthread(pthread_mutex_destroy(&mutex_estadisticas), "destroy estadisticas");
    free(hilos);
    free(pasajeros);
    free(asientos);
    return correcto ? EXIT_SUCCESS : EXIT_FAILURE;
}

