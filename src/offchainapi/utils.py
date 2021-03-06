# Copyright (c) The Libra Core Contributors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from os import urandom
import json

REQUIRED = True
OPTIONAL = False

WRITE_ONCE = True
UPDATABLE = False


class StructureException(Exception):
    pass


class StructureChecker:
    ''' A class that allows us to keep track of objects in terms of
    diffs, namely operations that mutate their fields. Also does
    automatic type checking and checking for mutability/immutability
    and required / optional fields.

    The actual fields of this object are held in a dictionary called
    `data`. However direct access through __getattr__ and membership
    quesries through __contains__ are supported, for ease of use.
    '''

    fields = {}

    def __init__(self):
        ''' Initialize the class. Presumes a class level variable
            fields is defined. '''
        assert self.fields
        self.data = {}
        self.update_record = []

    def __getattr__(self, name):
        ''' Provide a more humane interface to the data '''
        if name == "data":
            raise AttributeError()
        if name in self.data:
            return self.data[name]
        raise AttributeError(f"{self.__class__.__name__} does not have attribute {name}")

    def __contains__(self, item):
        ''' Allows for `in` boolean queries between attribute names
            and the object itself. '''
        return item in self.data

    def record(self, diff):
        ''' Record all diffs applied to the object '''
        self.update_record += [diff]

    def flatten(self):
        ''' Resets all diffs applied to this object '''
        self.update_record = []
        for field in self.data:
            if isinstance(self.data[field], StructureChecker):
                self.data[field].flatten()

    @classmethod
    def parse_map(cls):
        ''' Returns a map of fields to their respective type, and whether
            the type is a subclass of StructureChecker. '''
        parse_map = {
                field: (field_type, issubclass(field_type, StructureChecker))
                for field, field_type, _, _ in cls.fields
            }
        return parse_map

    def get_full_diff_record(self, diff=None):
        ''' Returns a hierarchy of diffs applied to this object and children'''
        parse = self.parse_map()
        if diff is None:
            diff = {}
        for field in self.data:
            xtype, parse_more = parse[field]
            if parse_more:
                diff[field] = self.data[field].get_full_diff_record()
            else:
                if xtype in {str, int, list, dict}:
                    diff[field] = self.data[field]
                else:
                    raise RuntimeError(f'Cannot get diff for type "{xtype}".')
        return diff

    def has_changed(self):
        ''' Returns True if the object has been modified.'''
        parse = self.parse_map()
        for new_diff in self.update_record:
            for field in new_diff:
                return True

        for field in self.data:
            _, parse_more = parse[field]
            if parse_more:
                if self.data[field].has_changed():
                    return True

        return False

    def __eq__(self, other):
        ''' Define equality as equality between data fields only '''
        if not isinstance(other, type(self)):
            return False
        if set(self.data) != set(other.data):
            return False
        for field, value in self.data.items():
            if not value == other.data[field]:
                return False
        return True

    @classmethod
    def from_full_record(cls, diff, base_instance=None):
        ''' Constructs an instance from a diff. '''

        if base_instance is None:
            self = cls.__new__(cls)
            StructureChecker.__init__(self)
        else:
            self = base_instance

        parse = cls.parse_map()
        new_diff = {}
        for field in diff:
            if field in parse:
                xtype, parse_further = parse[field]

                if parse_further:

                    if field in self.data:
                        # When the instance exists we update it in place, and
                        # We do not register this as a field update
                        # (to respect WRITE ONCE).
                        existing_instance = self.data[field]
                        self.data[field] = xtype.from_full_record(
                            diff[field], existing_instance)
                        new_diff[field] = self.data[field]
                    else:
                        new_diff[field] = xtype.from_full_record(diff[field])
                else:
                    # Use default constructor of the type
                    if xtype in {int, str, list, dict}:
                        new_diff[field] = xtype(diff[field])
                    else:
                        raise StructureException(f'Cannot parse type: {xtype}')

            else:
                # We tolerate fields we do not know about, but ignore them.
                pass

        self.update(new_diff)
        return self

    def custom_update_checks(self, diff):
        ''' Overwrite this class to implement more complex
            custom checks on a diff. '''
        pass

    def update(self, diff):
        ''' Applies changes to the object and checks for validity rules. '''
        # Check all types and write mode before update
        all_fields = set()
        updates = False
        for field, field_type, required, write_mode in self.fields:
            all_fields.add(field)
            if field in diff:

                # Check the type is right
                value = diff[field]
                if not isinstance(value, field_type):
                    actual_type = type(value)
                    raise StructureException(
                        f'Wrong type: field {field}, expected {field_type} '
                        f'but got {actual_type}'
                    )

                # Check you can write again
                if field in self.data and write_mode == WRITE_ONCE:
                    if self.data[field] != diff[field]:
                        raise StructureException(
                            f'Wrong update: field {field} cannot be changed'
                        )

        # Check we are not updating unknown fields
        for key in diff:
            if key not in all_fields:
                raise StructureException(f'Unknown: field {key}')

        # Finally update
        for key in diff:
            if key not in self.data or self.data[key] != diff[key]:
                self.data[key] = diff[key]
                updates = True

        for field, field_type, required, _ in self.fields:
            if required and field not in self.data:
                raise StructureException(f'Missing field: {field}')

        # Do custom checks on object
        self.custom_update_checks(diff)

        if updates:
            self.record(diff)


# define serializaqtion flags
class JSONFlag(Enum):
    """ A Flag denoting whether the JSON is intended
    for network transmission (NET) to another party or local storage
    (STORE). Some fields are private and are not serialized for NET."""
    NET = 'NET'
    STORE = 'STORE'


class JSONParsingError(Exception):
    """ Represents a JSON Parsing Error."""
    pass


class JSONSerializable:
    """ A Class that denotes a subclass is serializable, and
        provdes facilities to serialize and parse that class. """

    # Define a type map for decoding
    # It maps ObjectType attributes to a JSONSerializable subclass
    json_type_map = {}

    def get_json_data_dict(self, flag, update_dict=None):
        ''' Get a data dictionary compatible with JSON
            serilization (json.dumps) '''
        raise NotImplementedError()  # pragma: no cover

    @classmethod
    def from_json_data_dict(cls, data, flag, self=None):
        ''' Construct the object from a serlialized JSON data
            dictionary (from json.loads). '''
        raise NotImplementedError()  # pragma: no cover

    @classmethod
    def json_type(cls):
        ''' Overwrite this method to have a nicer json type identifier.'''
        return str(cls.__name__)

    @classmethod
    def register(cls, other_cls):
        """ A Class decorator to register subclasses as serializable. """
        cls.json_type_map[other_cls.json_type()] = other_cls
        return other_cls

    @classmethod
    def add_object_type(cls, value_dict):
        """ Registers the object type to the JSON dictionary. """
        assert '_ObjectType' not in value_dict
        value_dict['_ObjectType'] = cls.json_type()
        return value_dict

    @classmethod
    def parse(cls, data, flag):
        """Parse a data dictionary and return a JSON serializable instance. """
        if '_ObjectType' not in data:
            raise JSONParsingError('No object type information')

        if data['_ObjectType'] not in cls.json_type_map:
            raise JSONParsingError(
                f'Unknown object type: {data["_ObjectType"]}')

        new_cls = cls.json_type_map[data['_ObjectType']]
        return new_cls.from_json_data_dict(data, flag)

    def pretty(self, typex=JSONFlag.STORE):
        ''' Returns a string ready for pretty printing.'''
        parsed = self.get_json_data_dict(typex)
        s_pretty = json.dumps(parsed, indent=4, sort_keys=True)
        return s_pretty


# Utilities
def get_unique_string():
    ''' Returns a strong random 16 byte string encoded in hex. '''
    return urandom(16).hex()
