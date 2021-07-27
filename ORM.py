from numpy import ndarray
from datetime import datetime
import redis
import ast
import zlib


"""
vORM
A Lightweight Object Relation Model made for blazing fast python caching. 

Supports compression.

Written by Daniel Krivokuca (dan@voku.xyz) 2021
Under MIT License
"""


class CacheORM:
    def __init__(self, *args):
        """
        Sets up a simple, persistent cache in redis

        Parameters:
            - *args (CacheDefinition) :: Accepts n amount of CacheDefinitions
        """
        self.redis = redis.Redis("localhost", port=6379, db=0)
        self.utils = ORMUtils()
        self.is_compressed = False
        self.definitions = []
        self.def_keys = []

        # load the cache definitions
        for arg in args:
            if not isinstance(arg, CacheDefinition):
                raise Exception("All arguments must CacheDefinition classes")

            if arg._key() in self.def_keys:
                raise Exception(
                    "Multiple definitions with same key not allowed")

            self.definitions.append(arg)
            self.def_keys.append(arg._key())

    def get(self, cache_key, *val_keys):
        """
        Given a cache definitions key and the key for that cache, this function tries to return the given
        value if it exists. To retrieve ALL values, use the self.all() function

        Parameters:
            - cache_key (str) :: The key of the cache to access
            - val_keys (str) :: Key of the values to obtain.

        Returns:
            - results (False|dict) :: Returns a dict of keys given those keys exist. Else, False
        """

        if cache_key not in self.def_keys:
            return False

        result_dict = self._unpack(cache_key)
        if not result_dict:
            return False
        result_keys = list(result_dict.keys())
        results = {k: result_dict[k] for k in result_keys}
        return results

    def push(self, cache_key, **kvals):
        """
        Pushes a key-value pair to the database given the caches cache_key

        Parameters:
            - cache_key (str) :: The cache to which to write the keypairs to
            - **kvals (key=val) :: Unspecified amount of key values. Note that each key must exist
                                   in the CacheDefinition otherwise an unknown key error will be 
                                   thrown

        Returns:
            - True if the values were successfully pushed, exception if an unknown key error occured
        """

        # check to see if the cache exists
        if cache_key not in self.def_keys:
            raise Exception("Unknown cache_key")

        def_idx = self.def_keys.index(cache_key)
        def_repr = self.definitions[def_idx].repr

        cache_state = self._unpack(cache_key)

        if not cache_state:
            # cache isn't initialized in memory, initialize an empty cache
            cache_state = []

        cache_state_keys = list(self.definitions[def_idx]._dict())

        # type checking and appending
        app_dict = self.definitions[def_idx]._dict()
        for key in list(kvals.keys()):
            if key not in cache_state_keys:
                raise Exception("Unknown key `{}` passed".format(key))

            if not isinstance(kvals[key], def_repr[key]):
                raise Exception("Type mismatch with key `{}`".format(key))

            if isinstance(kvals[key], datetime):
                kvals[key] = datetime.strftime(kvals[key], "%Y-%m-%d %H:%M:%S")

            app_dict[key] = kvals[key]

        cache_state.append(app_dict)

        serialized_cache_state = self.utils.serialize_dict(cache_state)
        self.redis.set(cache_key, serialized_cache_state)
        return True

    def all(self, cache_key):
        """
        Returns an entire cache given a cache_key

        Parameters:
            - cache_key (str) :: The key of the cache to retrieve

        Returns:
            - cache(dict|None) :: If no cache exists, None is returned, else the cache is returned
        """
        return None

    def __repr__(self):
        rep = ""
        for definition in self.definitions:
            rep += definition.__repr__()
        return rep

    def _unpack(self, cache_key):
        """
        Given a cache key, this function unpacks the bytes and formats it as a dictionary before returning 
        to the user. 

        Parameters:
            - cache_key (str) :: Key of the CacheDefinition to unpack

        Returns:
            - unpacked (False|dict) :: False or dict depending on if the data exists. If it does exist, a 
                                       dictionary will be returned corresponding to the CacheDefinitions 
                                       **kwargs
        """
        if cache_key not in self.def_keys:
            return False

        # check redis
        raw_results = self.redis.get(cache_key)

        if not raw_results:
            return False

        unpacked = self.utils.deserialize_dict(
            raw_results, decompress=self.is_compressed)
        return unpacked


class CacheDefinition:

    # stuff allowed
    PRIMITIVES = [int, str, bool, list, float, dict, datetime]

    def __init__(self, cache_key, **kwargs):
        """
        Defines the schema of the cache object by accepting an arbitrary amount of key-value pairs.

        Parameters:
            - cache_key (str) :: The key of the cache (must be unique per cache).
            - **kwargs (key=type) :: Key value pairs. Each value must correspond to a type in the PRIMITIVES list
        """
        self.cache_key = cache_key
        self.hashkey = hash(self.cache_key)
        self.args = kwargs
        self.repr = {}
        for k in list(self.args.keys()):
            if self.args[k] not in self.PRIMITIVES:
                raise Exception("Unknown value passed")
            self.repr[k] = self.args[k]

    def __repr__(self):
        rep = "{}\n\n".format(self.cache_key)
        for k in list(self.args.keys()):
            rep += "{}\t{}\n".format(k, self.args[k])
        return rep

    def _key(self):
        """
        Returns the key of the CacheDefinition
        """
        return self.cache_key

    def _dict(self):
        """
        returns self.repr but with None as the value for each key
        """
        return {k: None for k in self.repr.keys()}


class BaseORM:
    def __init__(self, table_name, **kwargs):
        """
        This Base class abstracts away the query generation, type checking and other 
        important stuff. All ORM classes must inherit this class
        """
        self.__dict__.update(kwargs)


class Column:
    """
    Columns can only be used in tandem with the BaseORM class. For key-val cache storage, use the CacheORM
    and the CacheDefinition
    """

    def __init__(self, column_name, column_type, is_null=False, auto_increment=False, default=False):
        allowed_types = ['int', 'varchar', 'timestamp',
                         'text', 'bigint', 'long', 'float']
        if "(" in column_type:
            check = column_type[:column_type.index("(")]
        check = column_type
        assert check.lower() in allowed_types

        self.name = column_name
        self.type = column_type.upper()
        self.has_auto_increment = auto_increment
        self.is_null = is_null
        self.has_default_val = default

    def __call__(self):
        return {
            "column_name": self.name,
            "column_type": self.type,
            "is_null": self.is_null,
            "auto_increment": self.has_auto_increment,
            "default": self.has_default_val
        }

    def __str__(self):
        null_operator = " NOT NULL" if not self.is_null else ""
        ai_operator = " AUTO_INCREMENT" if self.has_auto_increment else ""
        default_operator = f" DEFAULT {self.has_default_val}" if self.has_default_val else ""
        return f"`{self.name}` {self.type}{null_operator}{ai_operator}{default_operator}"


class ORMUtils:
    """
    Useful functions
    """

    def serialize_dict(self, d, compress=False):
        string = str(d)
        string_bin = string.encode()
        if compress:
            string_bin = zlib.compress(string_bin)
        return string_bin

    def deserialize_dict(self, serialized_dict, decompress=False):
        if decompress:
            serialized_dict = zlib.decompress(serialized_dict)
        d = serialized_dict.decode('utf-8')
        decoded_dict = ast.literal_eval(d)
        return decoded_dict
