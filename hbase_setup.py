# hbase_setup.py — Tables HBase Diak-SN
import time

import happybase
import logging
import socket
import subprocess

logger = logging.getLogger('HBaseSetup')

socket.setdefaulttimeout(30)


def create_diak_sn_tables():
    conn = happybase.Connection(
        host='localhost',
        port=9090,
        timeout=30000,
    )
    # Supprimez conn.open() - déjà fait automatiquement
    
    subprocess.run(
    ['docker', 'exec', 'hbase', 'hbase', 'shell', '-n', '-c', "create_namespace 'diak'"],
    capture_output=True
   )
    
    tables = {
        'diak:cas_temps_reel': {'meta': dict(max_versions=1)},
        'diak:alertes_epidemie': {'meta': dict(max_versions=1)},
        'diak:stocks_medicaments': {'meta': dict(max_versions=1)},
    }
    
    try:
        existantes = [t.decode() for t in conn.tables()]
    except Exception as e:
        print("Erreur lecture tables :", e)
        existantes = []

    for nom, fam in tables.items():
        if nom not in existantes:
            conn.create_table(nom, fam)
            print(f"Table créée : {nom}")
        else:
            print(f"Table existe déjà : {nom}")

    conn.close()

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)

    create_diak_sn_tables()