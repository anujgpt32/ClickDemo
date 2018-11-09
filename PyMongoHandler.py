import pymongo


class PyMongoHandler:
    """
    module to handle connection to DB and stuff.
    """
    def __init__(self, db, host="localhost", port=27017, username="", password=""):
        self.username = username
        self.password = password
        # Creates the connection between MongoDB and the client.
        self.client = pymongo.MongoClient(host, port)
        self.db = self.connect(db)

    @property
    def get_db_instance(self):
        return self.db

    def check_db_existence(self, db):
        return db in self.client.list_database_names()

    def check_collection_existence(self, collection):
        return collection in self.db.list_collection_names()

    def create_collection(self, collection):
        """
        function to create the collection if doesn't exist already.
        :param collection:
        :return:
        """
        if not self.check_collection_existence(collection):
            self.db.create_collection(collection)
            return {"success": 1, "message": "Collection Created"}
        return {"success": 1, "message": "Collection Already Exists"}

    def connect(self, db):
        """
        returns the Database Instance
        :param db:
        :return:
        """
        return self.client[db] if self.check_db_existence(db) else None
