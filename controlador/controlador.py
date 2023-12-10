import threading
import time
from typing import Self
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
    def __init__(self):
        super().__init__()
        self.backup_server_address = ("localhost", 18863)
        self.backup_conn = None
        self.failover_thread = threading.Thread(target=self.check_backup_connection, daemon=True)
        self.is_failover = False
        self.failover_thread.start()

    def on_connect(self, conn):
        print("Conexão estabelecida com o servidor 1.")
        if self.is_failover:
            print("Retornando ao servidor principal.")
            self.is_failover = False
            self.failover_thread = threading.Thread(target=self.check_backup_connection, daemon=True)
            self.failover_thread.start()

    def on_disconnect(self, conn):
        print("Conexão encerrada com o servidor 1.")
        if not self.is_failover:
            print("Iniciando failover para o servidor de backup.")
            self.is_failover = True
            self.backup_conn = rpyc.connect(*self.backup_server_address)
        
    def check_backup_connection(self):
        while True:
            if self.is_failover:
                continue

            if self.backup_conn is None or not self.backup_conn.closed:
                try:
                    self.backup_conn = rpyc.connect(*self.backup_server_address)
                    print("Conexão de backup estabelecida.")
                except ConnectionRefusedError:
                    print("Falha ao conectar ao servidor de backup.")
            time.sleep(5)

    def exposed_acionar_alarme(self):
        try:
            if self.is_failover:
                print("Failover ativo: Executando acionar_alarme no servidor de backup.")
                return self.backup_conn.root.exposed_acionar_alarme()
            else:
                print("Executando acionar_alarme no servidor principal.")
                acao = "Ligar"
                acao_bytes = acao.encode('utf-8')
                encrypted_value = cipher_suite.encrypt(acao_bytes)

                self.exposed_trigger_callback("01")
            
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
        except Exception as e:
            print(f"Erro ao acionar o alarme: {e}")

    def exposed_desativar_alarme(self):
        try:
            if self.is_failover:
                print("Failover ativo: Executando desativar_alarme no servidor de backup.")
                return self.backup_conn.root.exposed_desativar_alarme()
            else:
                print("Executando desativar_alarme no servidor principal.")
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
        except Exception as e:
            print(f"Erro ao desativar o alarme: {e}")

    def exposed_aproximacao(self, intensidade):

        decyphred_value = cipher_suite.decrypt(intensidade).decode()
        print(decyphred_value)


if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer

    t = ThreadedServer(Controlador1Service, port=18862)
    t.start()

