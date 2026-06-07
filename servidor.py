import argparse
import json
import logging
import socket
import sys
import threading
from queue import Queue

WORKERS = 4

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def enviar_json(conn: socket.socket, data: dict) -> None:
    mensaje = (json.dumps(data, ensure_ascii=False) + "\n").encode("utf-8")
    conn.sendall(mensaje)


def recibir_linea(conn: socket.socket) -> bytes:
    buffer = b""
    while b"\n" not in buffer:
        chunk = conn.recv(4096)
        if not chunk:
            raise ConnectionError("Conexión cerrada antes de completar el mensaje")
        buffer += chunk
    return buffer.split(b"\n", 1)[0]


def recibir_json(conn: socket.socket) -> dict:
    return json.loads(recibir_linea(conn).decode("utf-8"))


# --- Worker ---

task_queue: Queue = Queue()


def procesar_tarea(payload: dict) -> dict:
    operacion = payload.get("operacion")
    datos = payload.get("datos")

    if operacion == "sumar":
        if not isinstance(datos, list) or not all(
            isinstance(n, (int, float)) for n in datos
        ):
            raise ValueError('La operación "sumar" requiere una lista numérica en "datos"')
        return {"resultado": sum(datos)}

    if operacion == "invertir":
        if not isinstance(datos, str):
            raise ValueError('La operación "invertir" requiere un texto en "datos"')
        return {"resultado": datos[::-1]}

    if operacion == "mayusculas":
        if not isinstance(datos, str):
            raise ValueError('La operación "mayusculas" requiere un texto en "datos"')
        return {"resultado": datos.upper()}

    if operacion == "ping":
        return {"resultado": "pong"}

    raise ValueError(f'Operación no soportada: "{operacion}"')


def worker_loop(worker_id: int, servidor_id: int) -> None:
    while True:
        conn, addr, payload = task_queue.get()
        try:
            logger.info(
                "Servidor %s - hilo %s procesando tarea de %s",
                servidor_id,
                worker_id,
                addr,
            )
            resultado = procesar_tarea(payload)
            respuesta = {
                "estado": "ok",
                "servidor": servidor_id,
                "worker": worker_id,
                **resultado,
            }
        except ValueError as exc:
            respuesta = {
                "estado": "error",
                "servidor": servidor_id,
                "worker": worker_id,
                "error": str(exc),
            }
        except Exception as exc:
            respuesta = {
                "estado": "error",
                "servidor": servidor_id,
                "worker": worker_id,
                "error": f"Error interno: {exc}",
            }

        try:
            enviar_json(conn, respuesta)
        except OSError:
            logger.warning("No se pudo responder al cliente %s", addr)
        finally:
            conn.close()
            task_queue.task_done()


def manejar_cliente_worker(conn: socket.socket, addr) -> None:
    try:
        payload = recibir_json(conn)
        logger.info("Tarea recibida de %s: %s", addr, payload)
        task_queue.put((conn, addr, payload))
    except (json.JSONDecodeError, UnicodeDecodeError):
        enviar_json(conn, {"estado": "error", "error": "JSON inválido"})
        conn.close()
    except ConnectionError as exc:
        logger.warning("Conexión incompleta desde %s: %s", addr, exc)
        conn.close()
    except OSError as exc:
        logger.error("Error de socket con %s: %s", addr, exc)
        conn.close()


def iniciar_pool(servidor_id: int) -> None:
    for worker_id in range(1, WORKERS + 1):
        threading.Thread(
            target=worker_loop,
            args=(worker_id, servidor_id),
            daemon=True,
            name=f"servidor-{servidor_id}-worker-{worker_id}",
        ).start()


def ejecutar_worker(args: argparse.Namespace) -> None:
    iniciar_pool(args.id)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((args.host, args.port))
        server.listen(10)
        logger.info(
            "Servidor worker %s escuchando en %s:%s (%s hilos)",
            args.id,
            args.host,
            args.port,
            WORKERS,
        )

        while True:
            conn, addr = server.accept()
            logger.info("Servidor worker %s: cliente conectado %s", args.id, addr)
            threading.Thread(
                target=manejar_cliente_worker,
                args=(conn, addr),
                daemon=True,
            ).start()


# --- Balanceador ---

selector_lock = threading.Lock()
selector_index = 0


def elegir_worker(workers: list[tuple[str, int]]) -> tuple[str, int]:
    global selector_index
    with selector_lock:
        worker = workers[selector_index % len(workers)]
        selector_index += 1
        return worker


def reenviar_tarea(worker_host: str, worker_port: int, mensaje: bytes) -> bytes:
    with socket.create_connection((worker_host, worker_port), timeout=10) as worker_conn:
        worker_conn.sendall(mensaje + b"\n")
        return recibir_linea(worker_conn)


def manejar_cliente_balanceador(
    conn: socket.socket, addr, workers: list[tuple[str, int]]
) -> None:
    try:
        mensaje = recibir_linea(conn)
        worker_host, worker_port = elegir_worker(workers)
        logger.info(
            "Balanceador: tarea de %s -> worker %s:%s",
            addr,
            worker_host,
            worker_port,
        )
        respuesta = reenviar_tarea(worker_host, worker_port, mensaje)
        conn.sendall(respuesta + b"\n")
    except ConnectionError as exc:
        logger.warning("Error con cliente %s: %s", addr, exc)
        conn.sendall(b'{"estado":"error","error":"No se pudo procesar la tarea"}\n')
    except OSError as exc:
        logger.error("Error de red con %s: %s", addr, exc)
    finally:
        conn.close()


def ejecutar_balanceador(args: argparse.Namespace) -> None:
    worker_ports = [int(p.strip()) for p in args.workers.split(",") if p.strip()]
    workers = [("127.0.0.1", port) for port in worker_ports]

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((args.host, args.port))
        server.listen(20)
        logger.info(
            "Balanceador escuchando en %s:%s -> workers %s",
            args.host,
            args.port,
            worker_ports,
        )

        while True:
            conn, addr = server.accept()
            threading.Thread(
                target=manejar_cliente_balanceador,
                args=(conn, addr, workers),
                daemon=True,
            ).start()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Servidor distribuido: balanceador y workers"
    )
    subparsers = parser.add_subparsers(dest="modo", required=True)

    parser_worker = subparsers.add_parser(
        "worker", help="Iniciar un servidor worker con pool de hilos"
    )
    parser_worker.add_argument("--host", default="127.0.0.1")
    parser_worker.add_argument("--port", type=int, required=True)
    parser_worker.add_argument(
        "--id", type=int, required=True, help="Identificador del servidor worker"
    )
    parser_worker.set_defaults(func=ejecutar_worker)

    parser_balanceador = subparsers.add_parser(
        "balanceador", help="Iniciar el balanceador de carga"
    )
    parser_balanceador.add_argument("--host", default="0.0.0.0")
    parser_balanceador.add_argument("--port", type=int, default=9000)
    parser_balanceador.add_argument(
        "--workers",
        default="9001,9002,9003",
        help="Puertos de los servidores worker separados por coma",
    )
    parser_balanceador.set_defaults(func=ejecutar_balanceador)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
