import random
import time

import paho.mqtt.client as mqtt
from cryptography.fernet import Fernet

encryption_key = 'g5rpWlh1x0cs27Uh9jf5Hs_GMUn5bPp-b_QfB_3h0jg='
cipher_suite = Fernet(encryption_key)

mqtt_broker = "localhost"
mqtt_topic = "/sensor"

mqtt_client_sensor = mqtt.Client()
mqtt_client_sensor.connect(mqtt_broker, 1883, 60)


def on_publish(client, userdata, result):
    print("Dispositivo 1: Dados Publicados.")
    pass


while True:
    time.sleep(1)
    intensidade = random.randint(0, 100)
    print(intensidade)
    
    # Converta o valor da intensidade para bytes antes de criptografar
    intensidade_bytes = str(intensidade).encode('utf-8')
    encrypted_value = cipher_suite.encrypt(intensidade_bytes)
    
    resultado = mqtt_client_sensor.publish(mqtt_topic, encrypted_value)
    mqtt_client_sensor.on_publish = on_publish
