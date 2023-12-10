# client.py
import rpyc

def callback_function(data):

    print(f"Função de callback no cliente chamada com dados: {data}")

if __name__ == "__main__":
    conn = rpyc.connect("localhost", 18862)
    server = conn.root

    conn = rpyc.connect("localhost", 18863)
    server = conn.root

    # Configurando a função de callback no servidor
    server.exposed_set_callback(callback_function)

    # Fazendo uma chamada no servidor que, por sua vez, chama a função de callback no cliente
    server.exposed_trigger_callback("Dados para o callback")
