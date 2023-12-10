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

    def exposed_acionar_alarme(self):
        #Esse try é para verificar se o controlador 1 está ligado
        try:
        
            conn = rpyc.connect("localhost", 18862)
        
            if conn.root.exposed_get_status() == "Ligado":
                print("Alarme já está ligado!")
        
        except ConnectionRefusedError:

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
        try:
        
            conn = rpyc.connect("localhost", 18862)
        
            if conn.root.exposed_get_status() == "Ligado":
                print("Alarme já está ligado!")
        
        except ConnectionRefusedError:

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

    def exposed_aproximacao(self, intensidade):

        decyphred_value = cipher_suite.decrypt(intensidade).decode()
        print(decyphred_value)


if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer

    t = ThreadedServer(Controlador1Service, port=18863)
    t.start()