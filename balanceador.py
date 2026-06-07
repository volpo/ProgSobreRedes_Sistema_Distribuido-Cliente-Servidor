import argparse
import logging
import socket
import threading

from protocolo import recibir_linea

HOST = "0.0.0.0"
PORT = 9000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)
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


def manejar_cliente(conn: socket.socket, addr, workers: list[tuple[str, int]]) -> None:
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Balanceador de carga (simula Nginx/HAProxy)"
    )
    parser.add_argument("--host", default=HOST)
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument(
        "--workers",
        default="9001,9002,9003",
        help="Puertos de los servidores worker separados por coma",
    )
    args = parser.parse_args()

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
                target=manejar_cliente,
                args=(conn, addr, workers),
                daemon=True,
            ).start()


if __name__ == "__main__":
    main()
