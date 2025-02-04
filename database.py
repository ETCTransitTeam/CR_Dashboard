import pymysql

class DatabaseConnector:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = None

    def connect(self):
        try:
            self.connection = pymysql.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            print("Connected to the database")
        except pymysql.MySQLError as e:
            print("Error connecting to the database:", e)

    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Disconnected from the database")
