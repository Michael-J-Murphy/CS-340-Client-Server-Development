"""
CRUD_Python_Module.py

Description: 
CS-340, Rescue Animal Database Project

Database interface module: implements standard CRUD database access

Object instantiation accepts DB connection and user/password information

No input validation is done in this class, so it is vulnerable to injection attacks.

Author: Michael Murphy
Date: 2025 09-14
"""

from pymongo import MongoClient 
from bson.objectid import ObjectId 


class AnimalShelter(object): 
    """ CRUD operations Class for Animal collection in MongoDB

    AnimalShelter Class 
    Constructor:
    __init__(self, username, password, host, port, database, collection):
    
    CRUD Functions:
    create(self, data)
    read(self, query)
    update(self, query, changed_fields)
    delete(self, query)

    """

    # Constructor
    def __init__(self, username, password, host, port, database, collection):
        """
        Initialize a connection to a MongoDB database and collection.

        :param username: MongoDB username.
        :type username: str
        :param password: MongoDB password.
        :type password: str
        :param host: Hostname or IP address of the MongoDB server.
        :type host: str
        :param port: Port number of the MongoDB server.
        :type port: int
        :param database: Name of the database to connect to.
        :type database: str
        :param collection: Name of the collection to use within the database.
        :type collection: str
        :return: None
        """
        
        # Initialize Connection 
        # Set the connection data based on the variables passed into the object
        self.client = MongoClient('mongodb://%s:%s@%s:%d' % (username,password,host,port)) 
        self.database = self.client['%s' % (database)] 
        self.collection = self.database['%s' % (collection)] 

    # Return the next available record number for use in the create method
    def get_next_rec_num(self):
        """ Get and return the next integer rec_num value.

        Searches the collection for existing records where `rec_num`
        is an integer, finds the highest value, and returns the next integer.
        Non-integer `rec_num` values are ignored. 
        If no integer `rec_num` values exist, the method returns 1.

        :return: The next available integer `rec_num`.
        :rtype: int
        """

        cursor = self.collection.find(
            {"rec_num": {"$type": "int"}},      # Only search for integer values
            sort=[("rec_num", -1)],             # Do a reverse order sort to find the last one
            limit=1                             # just return the last record
        )

        record = next(cursor, None)             # get the first (only) record, or None if no records are found

        if record and "rec_num" in record:      # verify that both record and rec_num field exists
            return record["rec_num"] + 1        # Increment and return the found rec_num
        else:
            return 1                            # Start at 1 if no records exist
  
    # Create method
    def create(self, data):
        """
        Insert a new record into the MongoDB collection.

        Automatically assigns the next available integer `rec_num`
        to the record, overwriting any `rec_num` provided in the input data.
        The input `data` must be a non-empty dictionary. 
        If `data` is `None`, an exception is raised.
        
        No input validation is done here, so this function in vulnerable to injection attacks.

        :param data: A dictionary containing the record fields and values.
        :type data: dict
        :return: True if the record was successfully inserted, False otherwise.
        :rtype: bool
        :raises Exception: If `data` is None or empty.
        """

        if data is not None: 
            data["rec_num"] = self.get_next_rec_num()               # Ensure that rec_num is the latest value; overwrites any value provided in the data
            try:
                result = self.collection.insert_one(data)           # data should be dictionary
                return result.acknowledged                          # Return “True” if successful insert, else “False”
            except PyMongoError:
                return False                                        # On Exception, return "False" for failed insert
        else: 
            raise Exception("Nothing to save, because data parameter is empty")

    # Read method
    def read(self, query):
        """
        Retrieve documents from the MongoDB collection based on a query.

        Executes a `find` operation using the provided query
        dictionary and returns a cursor to the matching documents. Results
        are sorted in ascending order by `rec_num`.
        
        No input validation is done here, so this function in vulnerable to injection attacks.

        :param query: MongoDB query as a dictionary to filter documents.
        :type query: dict
        :return: A cursor to the documents matching the query.
        :rtype: pymongo.cursor.Cursor
        """

        try:
            cursor = self.collection.find(
            query,                      # Use the query specified. !!! Needs validation to avoid injection vulnerability !!!
                sort=[("rec_num", 1)]   # Sort on rec_num, returning oldest (lowest record number first)
            )
            return cursor
        except Exception as e:
            print(f"Query failed: {e}")
            return []                   # On excpetion, return empty record set
   
    # Update method
    def update(self, query, changed_fields):
        """
        Update one or more documents in the collection.
        
        No input validation is done here, so this function in vulnerable to injection attacks.
        
        :param query: A dictionary used to select documents to update.
        :type query: dict
        :param changed_fields: A dictionary of fields to update, wrapped with '$set'.
        :type changed_fields: dict
        :return: The number of documents modified.
        :rtype: int

        """

        result = self.collection.update_many(query, {"$set": changed_fields})
        return result.modified_count
    
    # Delete method
    def delete(self, query):
        """
        Delete one or more documents in the collection.
        
        No input validation is done here, so this function in vulnerable to injection attacks.
        
        :param query: A dictionary used to select documents to delete.
        :type query: dict
        :return: The number of documents deleted.
        :rtype: int

        """

        result = self.collection.delete_many(query)
        return result.deleted_count

