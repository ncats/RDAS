import os
import threading
from mysql.connector import pooling
from gqlalchemy import Memgraph
from dotenv import load_dotenv
load_dotenv()

'''
Pools are initialized lazily after the launcher loads its environment. Alert
and GARD use the same factory but have separate Python processes and database
settings. Never create one pool for every DBConnection() instance: those small
factory objects are recreated by almost every task.
'''
_MYSQL_POOLS = {}
_MYSQL_POOL_LOCK = threading.Lock()


def _reset_mysql_pool_lock_after_fork():

    '''
    A forked worker must not inherit a lock held by another parent thread.
    Pool keys below include the PID, so workers create their own connections.
    Keep inherited entries referenced but unused: disposing of their sockets
    in the child could interfere with a parent connection. Spawned workers
    naturally import this module with an empty registry.
    '''
    global _MYSQL_POOL_LOCK
    _MYSQL_POOL_LOCK = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_mysql_pool_lock_after_fork)


class DBConnection:

    def __init__(self):

        pass
        

    def memgraph_conn(self):

        try:
            return Memgraph(
                host=os.getenv("MEMGRAPH_HOST"),
                port=int(os.getenv("MEMGRAPH_PORT")),
                username=os.getenv("MEMGRAPH_USERNAME"),
                password=os.getenv("MEMGRAPH_PASSWORD"),
                encrypted=True
            ) 
        except ValueError as ve:
            print(f"ValueError during port conversion: {ve}. Ensure MEMGRAPH_PORT is a valid integer.")
        except ConnectionRefusedError as cre:
            print(f"Connection refused: {cre}. Check if Memgraph is running on {os.getenv('MEMGRAPH_HOST')}:{os.getenv('MEMGRAPH_PORT')}.")
        except OSError as ose:
            print(f"OS Error: {ose}. This might indicate network issues or an incorrect host.")
        except Exception as e:
            print(f"An unexpected error occurred during Memgraph connection: {e}")


    def mysql_conn(self):

        """Borrow a connection from this process's pool for the active settings.

        For example, .env.alert selects rdas_db and .env.newdb selects
        gard_rdas_db. Configuration-specific keys prevent either database's
        connections from being handed to the other, even in one process.
        Credentials stay in memory and are never included in pool names/logs.

        Call close() once when database work ends; it returns the handle to the
        pool and resets the session, including unfinished transactions, without
        committing them. A released handle must not be reused. get_connection()
        checks connectivity and reconnects stale idle connections on checkout.
        Acquisition errors propagate to callers so a missing connection cannot
        silently advance pipeline checkpoints or clear pending is_new flags.
        """
        try:
            pool_size = int(os.getenv("MYSQL_POOL_SIZE", "5"))
        except ValueError as exc:
            raise ValueError(f"MYSQL_POOL_SIZE must be an integer from 1 to {pooling.CNX_POOL_MAXSIZE}.") from exc
        if not 1 <= pool_size <= pooling.CNX_POOL_MAXSIZE:
            raise ValueError(f"MYSQL_POOL_SIZE must be between 1 and {pooling.CNX_POOL_MAXSIZE}.")

        config = {
            "host": os.environ.get("MYSQL_HOST"),
            "port": int(os.getenv("MYSQL_PORT", "3306")),
            "user": os.environ.get("MYSQL_USER"),
            "password": os.environ.get("MYSQL_PASSWORD"),
            "database": os.environ.get("MYSQL_DATABASE"),
            "collation": "utf8mb4_unicode_ci",
            "charset": "utf8mb4",
        }
        pool_key = (os.getpid(), tuple(sorted(config.items())), pool_size)
        with _MYSQL_POOL_LOCK:
            pool = _MYSQL_POOLS.get(pool_key)
            if pool is None:
                '''
                Only pool creation is serialized here; Connector/Python handles
                concurrent checkouts itself. Five slots cover the sequential
                runners' selector/discovery/task connections by default. This
                limit is per process, including grant workers, and configurable
                for runs that hold more connections simultaneously.
                '''
                pool = pooling.MySQLConnectionPool(pool_name=f"rdas_{os.getpid()}_{len(_MYSQL_POOLS) + 1}", pool_size=pool_size, pool_reset_session=True, **config)
                _MYSQL_POOLS[pool_key] = pool
        return pool.get_connection()
