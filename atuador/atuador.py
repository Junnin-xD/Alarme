import paho.mqtt.client as mqtt
from cryptography.fernet import Fernet
encryption_key = 'g5rpWlh1x0cs27Uh9jf5Hs_GMUn5bPp-b_QfB_3h0jg='
cipher_suite = Fernet(encryption_key)


mqtt_broker = "localhost"
mqtt_topic = "/atuador"

mqtt_client_atuador = mqtt.Client()
mqtt_client_atuador.connect(mqtt_broker, 1883, 60)


def on_message(client, userdata, msg):
    decrypted_value = cipher_suite.decrypt(msg.payload)
    acao = decrypted_value.decode('utf-8')
    if acao == "Ligar":
        print(acao)

    elif acao == "Desligar":
        print(acao)


mqtt_client_atuador.subscribe(mqtt_topic)
mqtt_client_atuador.on_message = on_message
mqtt_client_atuador.loop_forever()
