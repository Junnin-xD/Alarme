import paho.mqtt.client as mqtt
import rpyc
from cryptography.fernet import Fernet
from datetime import datetime
from uuid import uuid4

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

encryption_key = 'g5rpWlh1x0cs27Uh9jf5Hs_GMUn5bPp-b_QfB_3h0jg='
cipher_suite = Fernet(encryption_key)


mqtt_broker = "localhost"
mqtt_topic_pub_atuador = "/atuador"
mqtt_topic_pub_sensor = "/sensor"

mqtt_client_controlador = mqtt.Client()
mqtt_client_controlador.connect(mqtt_broker, 1883, 60)

cluster = Cluster(['172.18.0.3', '172.18.0.2'])
session = cluster.connect("mikeyspace")

insert_query = SimpleStatement("INSERT INTO mikeyspace.alarme (id, status, data_hora) VALUES (%s, %s, %s);")


class Controlador1Service(rpyc.Service):

    def on_connect(self, conn):
        print("Conexão estabelecida com Controlador 1.")

    def on_disconnect(self, conn):
        print("Conexão perdida com Controlador 1.")

    #Metodo para retornar o status do controlador
    def exposed_get_status(self):
        return "Ligado"

    def exposed_acionar_alarme(self):
        acao = "Ligar"
        acao_bytes = acao.encode('utf-8')
        encrypted_value = cipher_suite.encrypt(acao_bytes)   

        try:
            mqtt_client_controlador.publish(mqtt_topic_pub_atuador, encrypted_value)

            data_hora = datetime.now()
            formato_string = "%Y-%m-%d %H:%M:%S"
            data_hora_string = data_hora.strftime(formato_string)
            print(data_hora_string)
            print("=====================")
            
            id_registro = uuid4()
            print(id_registro)
            print("=====================")

            print(encrypted_value)
            print("=====================")

            session.execute(insert_query, (id_registro, encrypted_value, data_hora_string))

        except ConnectionRefusedError:
            print("Sem conexão!!")

    def exposed_desativar_alarme(self):
        acao = "Desligar"
        acao_bytes = acao.encode('utf-8')
        encrypted_value = cipher_suite.encrypt(acao_bytes)

        try:
            mqtt_client_controlador.publish(mqtt_topic_pub_atuador, encrypted_value)

            data_hora = datetime.now()
            formato_string = "%Y-%m-%d %H:%M:%S"
            data_hora_string = data_hora.strftime(formato_string)
            print(data_hora_string)
            print("=====================")
            
            id_registro = uuid4()
            print(id_registro)
            print("=====================")

            print(encrypted_value)
            print("=====================")

            session.execute(insert_query, (id_registro, encrypted_value, data_hora_string))

        except ConnectionRefusedError:
            print("Banco cagado!!")

    def on_message(client, userdata, msg):

        decrypted_value = cipher_suite.decrypt(msg.payload)
        acao = decrypted_value.decode('utf-8')
        if acao == "Ligar":
            print(acao)

        elif acao == "Desligar":
            print(acao)

        return acao

    def exposed_aproximacao(self):
        acao = "Desligar"
        mqtt_client_sensor = mqtt.Client()
        mqtt_client_sensor.connect(mqtt_broker, 1883, 60)
    
        while acao == "Desligar":

            mqtt_client_sensor.subscribe(mqtt_topic_pub_sensor)
            mqtt_client_sensor.on_message = Controlador1Service.on_message()
            acao = mqtt_client_sensor.on_message
            print(acao)



if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer

    t = ThreadedServer(Controlador1Service, port=18862)
    t.start()