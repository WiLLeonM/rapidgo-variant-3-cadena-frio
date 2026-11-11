import json
import time
import uuid
import random
import os
from datetime import datetime, timezone
from kafka import KafkaProducer

# Configuración
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'eventos-cadena-frio'
EQUIPO = 'equipo-7'

# Rutas a los archivos JSON
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FILE_NORMAL = os.path.join(BASE_DIR, 'samples', 'evento_cadena_frio.json')
FILE_ALERTA = os.path.join(BASE_DIR, 'samples', 'evento_fuera_de_rango.json')

def cargar_y_actualizar_evento(filepath):
    """Lee el JSON base, actualiza el ID y la fecha, y retorna el dict."""
    with open(filepath, 'r', encoding='utf-8') as f:
        evento = json.load(f)

    # Actualizamos los datos dinámicos según el contrato JSON
    evento['evento_id'] = str(uuid.uuid4())
    evento['timestamp'] = datetime.now(timezone.utc).isoformat()
    
    # Asignamos un pedido aleatorio para que varíe (y podamos probar la Key)
    evento['pedido_id'] = f"PED-{random.randint(100, 999)}"
    
    return evento

def main():
    print(f"Iniciando productor para {EQUIPO} basado en archivos JSON...")
    
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8')
    )

    try:
        while True:
            # Alternamos entre enviar un evento normal y uno con alerta (80% normal, 20% alerta)
            if random.random() > 0.2:
                evento = cargar_y_actualizar_evento(FILE_NORMAL)
            else:
                evento = cargar_y_actualizar_evento(FILE_ALERTA)
            
            # La key es el pedido_id
            key = evento['pedido_id']
            
            producer.send(TOPIC, key=key, value=evento)
            print(f"Enviado: Key={key} | Temp={evento.get('temperatura_c')}°C | ID={evento['evento_id']}")
            
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nDeteniendo productor...")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
