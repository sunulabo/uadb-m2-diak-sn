# kafka_producer_diak.py — Simulateur cas médicaux + stocks 
from kafka import KafkaProducer 
import json, random, time, uuid 
from datetime import datetime 
import numpy as np 
  
random.seed(42); np.random.seed(42) 
DISTRICTS = ['DAKAR','THIES','KAOLACK','ZIGUINCHOR','SAINT_LOUIS','LOUGA','TAMBACOUNDA'] 
PATHOLOGIES = ['PALUDISME','DENGUE','CHOLERA','HTA','DIABETE','DIARRHEE','IRA'] 
# Saisonnalité : paludisme pic hivernage (juil-oct), choléra saison sèche 
PROB_SAISON = { 
    'PALUDISME': lambda m: 0.4 if 7<=m<=10 else 0.1, 
    'DENGUE':    lambda m: 0.3 if 8<=m<=11 else 0.05, 
    'CHOLERA':   lambda m: 0.2 if m in [1,2,3,4,5] else 0.05, 
    'HTA':       lambda m: 0.15, 'DIABETE': lambda m: 0.12, 
    'DIARRHEE':  lambda m: 0.2, 'IRA': lambda m: 0.18,
}
STOCK_BASE = {'COARTEM':500,'PARACETAMOL':2000,'SRO':800,'METFORMINE':300,'AMLODIPINE':400} 
  
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # OBLIGATOIRE
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(2, 0, 2)
)
  
def gen_cas(): 
    district  = random.choice(DISTRICTS) 
    m         = datetime.utcnow().month 
    pathologie= random.choices(PATHOLOGIES, 
                    weights=[PROB_SAISON[p](m) for p in PATHOLOGIES])[0] 
    return { 
        'cas_id':       str(uuid.uuid4()), 
        'patient_id':   f'PAT_{uuid.uuid4().hex[:10].upper()}',  # Sera anonymisé 
        'district':     district, 
        'pathologie':   pathologie, 
        'age_tranche':  random.choice(['0-5','6-14','15-49','50+']), 
        'sexe':         random.choice(['M','F']), 
        'gravite':      random.choice(['LEGER','MODERE','GRAVE']), 
        'timestamp':    datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'), 
    } 
  
def gen_stock(): 
    district  = random.choice(DISTRICTS) 
    medicament= random.choice(list(STOCK_BASE.keys())) 
    base      = STOCK_BASE[medicament] 
    return { 
        'district':     district, 
        'medicament':   medicament, 
        'stock_actuel': max(0, round(base * random.uniform(0.1, 1.5))), 
        'stock_mini':   round(base * 0.2), 
        'timestamp':    datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'), 
    } 
  
if __name__ == '__main__': 
    print('Simulateur Diak-SN démarré...') 
    while True: 
        for _ in range(5): producer.send('diak_cas_raw', gen_cas()) 
        producer.send('diak_stocks_raw', gen_stock()) 
        producer.flush(); time.sleep(3)