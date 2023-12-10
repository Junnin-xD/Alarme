import rpyc


class Controlador2Client:

    def realizar_comunicacao(self):

        entrada = input("Digite 1 para acionar o alarme, 2 para desativar o alarme ou 3 para detectar aproximação: ")

        if entrada == "1":
            try:
                self.conn = rpyc.connect("localhost", 18862)
                result = self.conn.root.acionar_alarme()
            except ConnectionRefusedError:
                self.conn02 = rpyc.connect("localhost", 18863)
                result = self.conn02.root.acionar_alarme()
        elif entrada == "2":
            try:
                self.conn = rpyc.connect("localhost", 18862)
                result = self.conn.root.desativar_alarme()
            except ConnectionRefusedError:
                self.conn02 = rpyc.connect("localhost", 18863)
                result = self.conn02.root.desativar_alarme()
        elif entrada == "3":
            try:
                self.conn = rpyc.connect("localhost", 18862)
                result = self.conn.root.aproximacao()
            except ConnectionRefusedError:
                self.conn02 = rpyc.connect("localhost", 18863)
                result = self.conn02.root.aproximacao()


if __name__ == "__main__":
    controlador2 = Controlador2Client()
    while True:
        controlador2.realizar_comunicacao()
