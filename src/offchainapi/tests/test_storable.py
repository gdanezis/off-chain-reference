# Copyright (c) The Libra Core Contributors
# SPDX-License-Identifier: Apache-2.0

# Tests for the storage framework
from ..storage import StorableDict, StorableValue, StorableFactory, BasicStore
from ..payment_logic import PaymentCommand
from ..protocol_messages import make_success_response, CommandRequestObject, \
    make_command_error
from ..errors import OffChainErrorCode

import pytest

def test_dict_basic():

    db = BasicStore.mem()
    db.can_write = True

    D = StorableDict(db, 'mary', int)
    assert D.is_empty()
    D['x'] = 10
    assert not D.is_empty()

    assert D['x'] == 10
    assert len(list(D.keys())) == 1
    D['hello'] = 2
    assert len(list(D.keys())) == 2
    del D['x']
    assert D['hello'] == 2
    assert len(list(D.keys())) == 1
    assert 'hello' in D
    assert 'x' not in D


def test_dict_dict():
    db = BasicStore.mem()
    db.can_write = True

    D = StorableDict(db, 'mary', int)
    assert list(D.keys()) == []

    D['x'] = 10
    D['y'] = 20
    D['z'] = 30
    D['a'] = 40
    D['b'] = 50

    assert len(list(D.keys())) == 5
    assert set(D.keys()) == {'x', 'y', 'z', 'a', 'b'}

    D = StorableDict(db, 'anna', int)
    D['x'] = 10
    D['y'] = 20
    D['z'] = 30
    D['a'] = 40
    D['b'] = 50

    del D['x']
    assert set(D.keys()) == {'y', 'z', 'a', 'b'}
    del D['b']
    assert set(D.keys()) == {'y', 'z', 'a'}

    assert set(D.values()) == {20, 30, 40}

def test_dict_dict_del_to_empty():
    db = BasicStore.mem()
    db.can_write = True

    D = StorableDict(db, 'to_del', bool)
    D['x'] = True
    del D['x']
    D['y'] = True
    del D['y']
    assert len(list(D.keys())) == 0


def test_dict_index():
    db = BasicStore.mem()
    db.can_write = True

    D = StorableDict(db, 'mary', int)
    D['x'] = 10
    assert D['x'] == 10
    assert len(list(D.keys())) == 1
    D['hello'] = 2
    assert len(list(D.keys())) == 2
    del D['x']
    assert D['hello'] == 2
    assert len(list(D.keys())) == 1
    assert 'hello' in D
    assert 'x' not in D


def test_hierarchy():
    db = BasicStore.mem()
    db.can_write = True

    val = StorableValue(db, 'counter', None)

    val2 = StorableDict(db, 'counter', int, root=val)

    val2['xx'] = 20
    assert val2['xx'] == 20


def test_value_payment(db, payment):
    db.can_write = True
    val = StorableDict(db, 'payment', payment.__class__)

    val['name'] = payment
    pay2 = val['name']
    assert payment == pay2

    D = StorableDict(db, 'mary', payment.__class__)
    D[pay2.version] = pay2
    assert D[pay2.version] == payment


def test_value_command(db, payment):
    db.can_write = True

    cmd = PaymentCommand(payment)

    val = StorableDict(db, 'command', PaymentCommand)
    val['name'] = cmd
    assert val['name'] == cmd

    cmd.writes_version_map = [('xxxxxxxx', 'xxxxxxxx')]
    assert val['name'] != cmd
    val['name'] = cmd
    assert val['name'] == cmd


def test_value_request(db, payment):
    db.can_write = True
    cmd = CommandRequestObject(PaymentCommand(payment))
    cmd.cid = '10'

    val = StorableDict(db, 'command', CommandRequestObject)
    val['name'] = cmd
    assert val['name'] == cmd

    cmd.response = make_success_response(cmd)
    assert cmd.response is not None
    assert val['name'] != cmd
    assert val['name'].response is None

    val['name'] = cmd
    assert val['name'] == cmd

    cmd.response = make_command_error(cmd, code=OffChainErrorCode.test_error_code)
    assert val['name'] != cmd
    val['name'] = cmd
    assert val['name'] == cmd

def test_dict_trans():
    db = BasicStore.mem()
    db.can_write = True
    store = StorableFactory(db)

    with store as _:
        eg = store.make_dict('eg', int, None)

    with store as _:
        eg['x'] = 10
        eg['x'] = 20
        eg['y'] = 20
        eg['x'] = 30

    with store as _:
        x = eg['x']
        #l = len(eg)
        eg['z'] = 20

    assert len(list(eg.keys())) == 3
    assert set(eg.keys()) == set(['x', 'y', 'z'])

def test_sqlstore():
    import sqlite3

    db = sqlite3.connect(':memory:')


    b = BasicStore(db)

    # Can open twice?
    b = BasicStore(db)

    # Check mem constructor
    b = BasicStore.mem()
    b.can_write = True

    with pytest.raises(KeyError):
        b.get('n0', 'k0')

    # Insert some values
    b.put('n0', 'k0', 'v0')
    assert b.get('n0', 'k0') == 'v0'

    # Ensure values change
    b.put('n0', 'k0', 'v1')
    assert b.get('n0', 'k0') == 'v1'

    # check isin operator
    assert b.isin('n0', 'k0') is True
    assert b.isin('n0', 'k1') is False

    # Check delete
    b.delete('n0', 'k0')
    with pytest.raises(KeyError):
        b.get('n0', 'k0')

    with pytest.raises(KeyError):
        b.delete('n0', 'k0')

    # Check iterators
    b.put('n1', 'k0', 'v0')
    b.put('n1', 'k1', 'v0')
    b.put('n1', 'k2', 'v0')

    assert set(b.getkeys('n1')) == {'k0', 'k1', 'k2'}
    assert set(b.getkeys('nx')) == set()

    # Test count
    assert b.count('n1') == 3
    assert b.count('nx') == 0

    # Test transactions
    b.put('n2', 'k0', 'v0')
    assert b.isin('n2', 'k0')

    b.end_transaction(commit=False)
    assert not b.isin('n2', 'k0')

    b.put('n2', 'k0', 'v0')
    b.end_transaction(commit=True)
    b.end_transaction(commit=False)
    assert b.isin('n2', 'k0')
