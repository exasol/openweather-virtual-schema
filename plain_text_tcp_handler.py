class PlainTextTcpHandler(logging.handlers.SocketHandler):
    """ Sends plain text log message over TCP channel """

    def makePickle(self, record):
        message = self.formatter.format(record) + "\r\n"
        return message.encode()

    @staticmethod
    def initialize_logger(ip: str, port: int, level):
        root_logger = logging.getLogger('')

        try:
            if level in ('INFO', 'WARNING', 20, 30):
                root_logger.setLevel(level)
            else:
                raise TypeError()
        except TypeError as e:
            e.message = "E-VS-OWFS-4 Chosen LOG_LEVEL not supported. Please choose 'INFO' or 'WARNING'"
            raise

        socket_handler = PlainTextTcpHandler(ip, port)
        socket_handler.setFormatter(logging.Formatter('%(asctime)s: %(message)s'))
        root_logger.addHandler(socket_handler)
        return root_logger