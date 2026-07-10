from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class DatabaseConnector:
    host: str
    database: str
    user: str
    password: str
    port: int = 3306
    connection: Optional[object] = None

    def connect(self) -> None:
        """
        MySQL connector using mysql-connector-python.

        Expected usage across this repo:
          - db = DatabaseConnector(host, database, user, password)
          - db.connect()
          - pd.read_sql(query, db.connection)
          - db.disconnect()
        """
        try:
            import mysql.connector  # type: ignore
        except Exception as e:  # pragma: no cover
            raise ImportError(
                "mysql-connector-python is required. Install with: pip install mysql-connector-python"
            ) from e

        self.connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port,
        )

    def disconnect(self) -> None:
        if self.connection is not None:
            try:
                self.connection.close()
            finally:
                self.connection = None

