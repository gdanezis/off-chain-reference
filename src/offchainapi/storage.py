# Copyright (c) The Libra Core Contributors
# SPDX-License-Identifier: Apache-2.0

# The main storage interface.

from hashlib import sha256
import json
import contextvars
from .utils import JSONFlag, JSONSerializable, get_unique_string
import sqlite3

class BasicStore:

    @staticmethod
    def mem():
        return BasicStore(sqlite3.connect(':memory:', check_same_thread=False))

    def __init__(self, db):
        self.db = db
        self.can_write = False

        try:
            self.db.execute("""CREATE TABLE kvstore (
                ns text NOT NULL,
                key text NOT NULL,
                val text NOT NULL)""")

            self.db.execute("""CREATE UNIQUE INDEX namespace_key
                            ON kvstore (ns, key)""")

            self.db.commit()
        except sqlite3.OperationalError:
            # Table exists
            pass

    def set_write(self, can_wrtie):
        self.can_write = can_wrtie

    def check_write(self):
        if not self.can_write:
            raise RuntimeError('Store cannot be used for writing outside a transaction.')

    def get(self, ns, key):
        sql = """SELECT val
                 FROM kvstore
                WHERE ns=? and key=?"""

        res = self.db.execute(sql, (ns, key, ))

        val = res.fetchone()
        if val is None:
            raise KeyError((ns, key))
        else:
            return val[0]

    def put(self, ns, key, val):
        self.check_write()

        sql = """INSERT OR REPLACE INTO kvstore (ns, key, val)
                 VALUES (?, ?, ?)"""

        res = self.db.execute(sql, (ns, key, val))

    def delete(self, ns, key):
        self.check_write()

        sql = """DELETE FROM kvstore
                 WHERE ns=? AND key=?"""

        res = self.db.execute(sql, (ns, key,) )
        if res.rowcount == 0:
            raise KeyError((ns, key))

    def isin(self, ns, key):
        try:
            self.get(ns, key)
            return True
        except KeyError:
            return False

    def getkeys(self, ns):
        sql = """SELECT key
                 FROM kvstore
                WHERE ns=?"""

        res = self.db.execute(sql, (ns, ))
        return list(r[0] for r in res)

    def count(self, ns):
        sql = """SELECT COUNT(key)
                 FROM kvstore
                WHERE ns=?"""

        res = self.db.execute(sql, (ns, ))
        return res.fetchone()[0]

    def end_transaction(self, commit=True):
        if commit:
            self.db.commit()
        else:
            self.db.rollback()


def key_join(strs):
    ''' Joins a sequence of strings to form a storage key. '''
    # Ensure this is parseable and one-to-one to avoid collisions.
    return '||'.join([f'[{len(s)}:{s}]' for s in strs])


class Storable:
    """Base class for objects that can be stored.

    Args:
        xtype (*): the type (or base type) of the objects to be stored.
    """

    def __init__(self, xtype):
        self.xtype = xtype
        self.factory = None


    def pre_proc(self, val):
        """ Pre-processing of objects before storage. By default
            it calls get_json_data_dict for JSONSerializable objects or
            their base type. eg int('10'). The result must be a structure
            that can be passed to json.dumps.
        """
        if issubclass(self.xtype, JSONSerializable):
            return val.get_json_data_dict(JSONFlag.STORE)
        else:
            return self.xtype(val)

    def post_proc(self, val):
        """ Post-processing to convert a json parsed structure into a Python
            object. It uses parse on JSONSerializable objects, and otherwise
            the type constructor.
        """
        if issubclass(self.xtype, JSONSerializable):
            return self.xtype.parse(val, JSONFlag.STORE)
        else:
            return self.xtype(val)


class StorableFactory:
    ''' This class maintains an overview of the full storage subsystem,
    and creates specific classes for values, lists and dictionary like
    types that can be stored persistently. It also provides a context
    manager to provide atomic, all-or-nothing crash resistent
    transactions.

    Initialize the ``StorableFactory`` with a persistent key-value
    store ``db``. In case the db already contains data the initializer
    runs the crash recovery procedure to cleanly re-open it.
    '''

    def __init__(self, db):
        self.db = db
        self.current_transaction = None
        self.levels = 0

    def make_dir(self, name, root=None):
        ''' Makes a new value-like storable.

            Parameters:
                * name : a string representing the name of the object.
                * xtype : the type of the object. It may be a simple type
                  or a subclass of JSONSerializable.
                * root : another storable object that acts as a logical
                  folder to this one.
                * default : the default value of the storable.

        '''

        v = StorableValue(self.db, name, root)
        v.factory = self
        return v

    def make_dict(self, name, xtype, root):
        ''' A new map-like storable object.

            Parameters:
                * name : a string representing the name of the object.
                * xtype : the type of the object stored in the map.
                  It may be a simple type or a subclass of
                  JSONSerializable. The keys are always strings.
                * root : another storable object that acts as a logical
                  folder to this one.

        '''
        v = StorableDict(self.db, name, xtype, root)
        v.factory = self
        return v

    # Define the interfaces as a context manager

    def atomic_writes(self):
        ''' Returns a context manager that ensures
            all writes in its body on the objects created by this
            StorableFactory are atomic.

            Attempting to write to the objects created by this
            StorableFactory outside the context manager will
            throw an exception. The context manager is re-entrant
            and commits to disk occur when the outmost context
            manager (with) exits.'''
        return self

    def __enter__(self):
        if self.levels == 0:
            self.current_transaction = get_unique_string()
            self.db.set_write(True)

        self.levels += 1

    def __exit__(self, exc_type, exc_value, traceback):
        self.levels -= 1
        if self.levels == 0:
            self.current_transaction = None
            self.db.set_write(False)
            if exc_type:
                self.db.end_transaction(commit=False)
                print('STORAGE ERROR Rollback', exc_type, exc_value)
            else:
                self.db.end_transaction(commit=True)



class StorableDict(Storable):
    """ Implements a persistent dictionary like type. Entries are stored
        by key directly, and a separate doubly linked list structure is
        stored to enable traversal of keys and values.

        Supports:
            * __getitem__(self, key)
            * __setitem__(self, key, value)
            * keys(self)
            * values(self)
            * __len__(self)
            * __contains__(self, item)
            * __delitem__(self, key)

        Keys should be strings or any object with a unique str representation.
        """

    def __init__(self, db, name, xtype, root=None):

        if root is None:
            self.root = ['']
        else:
            self.root = root.base_key()
        self.name = name
        self.db = db
        self.xtype = xtype

        self.ns = sha256(key_join(self.base_key()).encode('utf8')).digest().hex()

    def base_key(self):
        return self.root + [self.name]

    def __getitem__(self, key):
        return self.post_proc(json.loads(self.db.get(self.ns, key)))

    def __setitem__(self, key, value):
        data = json.dumps(self.pre_proc(value))
        self.db.put(self.ns, key, data)


    def keys(self):
        ''' An iterator over the keys of the dictionary. '''
        return self.db.getkeys(self.ns)

    def values(self):
        ''' An iterator over the values of the dictionary. '''
        for k in self.keys():
            yield self[k]

    def __len__(self):
        return self.db.count()

    def is_empty(self):
        ''' Returns True if dict is empty and False if it contains some elements.'''
        return self.db.count(self.ns) == 0


    def __delitem__(self, key):
        self.db.delete(self.ns, key)

    def __contains__(self, item):
        return self.db.isin(self.ns, item)


class StorableValue:
    """ Implements a cached persistent value. The value is stored to storage
        but a cached variant is stored for quick reads.
    """

    def __init__(self, db, name, root=None):
        if root is None:
            self.root = ['']
        else:
            self.root = root.base_key()

        self.name = name
        self.db = db
        self.ns = sha256(key_join(self.base_key()).encode('utf8')).digest().hex()

    def base_key(self):
        return self.root + [ self.name ]
