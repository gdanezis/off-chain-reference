from ..protocol import VASPPairChannel, make_protocol_error, NetMessage
from ..executor import ExecutorException, ProtocolExecutor
from ..protocol_messages import CommandRequestObject, CommandResponseObject, \
    OffChainProtocolError, OffChainException, OffChainOutOfOrder
from ..sample.sample_command import SampleCommand
from ..command_processor import CommandProcessor
from ..utils import JSONSerializable, JSONFlag
from ..storage import StorableFactory

import types
from copy import deepcopy
import random
from unittest.mock import MagicMock
from mock import PropertyMock
import pytest
import asyncio
import json


def monkey_tap_to_list(pair, requests_sent, replies_sent):
    pair.msg = []
    pair.xx_requests_sent = requests_sent
    pair.xx_replies_sent = replies_sent
    pair.xx_requests_stats = 0
    pair.xx_replies_stats = 0

    def to_tap_requests(self, msg):
        assert msg is not None
        assert isinstance(msg, CommandRequestObject)
        self.xx_requests_stats += 1
        self.xx_requests_sent += [deepcopy(msg)]

    def to_tap_reply(self, msg):
        assert isinstance(msg, CommandResponseObject)
        assert msg is not None
        self.xx_replies_stats += 1
        self.xx_replies_sent += [deepcopy(msg)]

    pair.send_request = types.MethodType(to_tap_requests, pair)
    pair.send_response = types.MethodType(to_tap_reply, pair)
    return pair


class RandomRun(object):
    def __init__(self, server, client, commands, seed='fixed seed'):
        # MESSAGE QUEUES
        self.to_server_requests = []
        self.to_client_response = []
        self.to_client_requests = []
        self.to_server_response = []

        self.server = monkey_tap_to_list(
            server, self.to_client_requests, self.to_client_response
        )
        self.client = monkey_tap_to_list(
            client, self.to_server_requests, self.to_server_response
        )

        self.commands = commands
        self.number = len(commands)
        random.seed(seed)

        self.DROP = True
        self.VERBOSE = False

    def run(self):
        to_server_requests = self.to_server_requests
        to_client_response = self.to_client_response
        to_client_requests = self.to_client_requests
        to_server_response = self.to_server_response
        server = self.server
        client = self.client
        commands = self.commands

        while True:

            # Inject a command every round
            if random.random() > 0.99:
                if len(commands) > 0:
                    c = commands.pop(0)
                    try:
                        if random.random() > 0.5:
                            client.sequence_command_local(c)
                        else:
                            server.sequence_command_local(c)
                    except ExecutorException:
                        commands.insert(0, c)

            # Random drop
            while self.DROP and random.random() > 0.3:
                kill_list = random.choice([to_server_requests,
                                           to_client_requests,
                                           to_client_response,
                                           to_server_response])
                del kill_list[-1:]

            Case = [False, False, False, False, False]
            Case[random.randint(0, len(Case) - 1)] = True
            Case[random.randint(0, len(Case) - 1)] = True

            # Make progress by delivering a random queue
            if Case[0] and len(to_server_requests) > 0:
                client_request = to_server_requests.pop(0)
                server.send_response(server.handle_request(client_request))

            if Case[1] and len(to_client_requests) > 0:
                server_request = to_client_requests.pop(0)
                client.send_response(client.handle_request(server_request))

            if Case[2] and len(to_client_response) > 0:
                rep = to_client_response.pop(0)
                # assert req.client_sequence_number is not None
                try:
                    client.handle_response(rep)
                except OffChainProtocolError:
                    pass
                except OffChainException:
                    raise
                except OffChainOutOfOrder:
                    pass

            if Case[3] and len(to_server_response) > 0:
                rep = to_server_response.pop(0)
                try:
                    server.handle_response(rep)
                except OffChainProtocolError:
                    pass
                except OffChainException:
                    raise
                except OffChainOutOfOrder:
                    pass

            # Retransmit
            if Case[4] and random.random() > 0.10:
                client.retransmit()
                server.retransmit()

            if self.VERBOSE:
                print([to_server_requests,
                       to_client_requests,
                       to_client_response,
                       to_server_response])

                print([server.would_retransmit(),
                       client.would_retransmit(),
                       server.executor.last_confirmed,
                       client.executor.last_confirmed])

            if not server.would_retransmit() and not client.would_retransmit() \
                    and server.executor.last_confirmed == self.number \
                    and client.executor.last_confirmed == self.number:
                break

    def checks(self, NUMBER):
        client = self.client
        server = self.server

        client_seq = [c.item() for c in client.get_final_sequence()]
        server_seq = [c.item() for c in server.get_final_sequence()]

        assert len(client_seq) == NUMBER
        assert client_seq == server_seq
        assert set(range(NUMBER)) == set(client_seq)

        client_exec_seq = [c.item() for c in client.executor.command_sequence]
        server_exec_seq = [c.item() for c in server.executor.command_sequence]
        assert set(client_seq) == set(client_exec_seq)
        assert set(server_seq) == set(server_exec_seq)


def test_create_channel_to_myself(three_addresses, vasp):
    a0, _, _ = three_addresses
    command_processor = MagicMock(spec=CommandProcessor)
    store = MagicMock()
    with pytest.raises(OffChainException):
        channel = VASPPairChannel(a0, a0, vasp, store, command_processor)


def test_client_server_role_definition(three_addresses, vasp):
    a0, a1, a2 = three_addresses
    command_processor = MagicMock(spec=CommandProcessor)
    store = MagicMock()

    channel = VASPPairChannel(a0, a1, vasp, store, command_processor)
    assert channel.is_server()
    assert not channel.is_client()

    channel = VASPPairChannel(a1, a0, vasp, store, command_processor)
    assert not channel.is_server()
    assert channel.is_client()

    # Lower address is server (xor bit = 1)
    channel = VASPPairChannel(a0, a2, vasp, store, command_processor)
    assert not channel.is_server()
    assert channel.is_client()

    channel = VASPPairChannel(a2, a0, vasp, store, command_processor)
    assert channel.is_server()
    assert not channel.is_client()


def test_protocol_server_client_benign(two_channels):
    server, client = two_channels

    # Create a server request for a command
    server.sequence_command_local(SampleCommand('Hello'))
    assert len(server.get_final_sequence()) > 0

    msg_list = server.tap()
    assert len(msg_list) == 1
    request = msg_list.pop()
    assert isinstance(request, CommandRequestObject)
    assert server.my_next_seq() == 1

    print()
    print(request.pretty(JSONFlag.NET))

    # Pass the request to the client
    assert client.other_next_seq() == 0
    reply = client.handle_request(request)
    assert isinstance(reply, CommandResponseObject)
    assert client.other_next_seq() == 1
    assert reply.status == 'success'

    print()
    print(reply.pretty(JSONFlag.NET))

    # Pass the reply back to the server
    assert len(server.executor.command_status_sequence) == 0
    server.handle_response(reply)
    msg_list = server.tap()
    assert len(msg_list) == 0  # No message expected

    assert len(server.executor.command_status_sequence) > 0
    assert len(client.executor.command_status_sequence) > 0
    assert client.get_final_sequence()[0].item() == 'Hello'


def test_protocol_server_conflicting_sequence(two_channels):
    server, client = two_channels

    # Create a server request for a command
    server.sequence_command_local(SampleCommand('Hello'))
    request = server.tap()[0]

    # Modilfy message to be a conflicting sequence number
    request_conflict = deepcopy(request)
    assert request_conflict.seq == 0
    request_conflict.command = SampleCommand("Conflict")

    # Pass the request to the client
    reply = client.handle_request(request)
    reply_conflict = client.handle_request(request_conflict)

    # We only sequence one command.
    assert client.other_next_seq() == 1
    assert reply.status == 'success'

    # The response to the second command is a failure
    assert reply_conflict.status == 'failure'
    assert reply_conflict.error.code == 'conflict'

    print()
    print(reply_conflict.pretty(JSONFlag.NET))

    # Pass the reply back to the server
    assert len(server.executor.command_status_sequence) == 0
    server.handle_response(reply)
    msg_list = server.tap()
    assert len(msg_list) == 0  # No message expected

    assert len(server.executor.command_status_sequence) > 0
    assert len(client.executor.command_status_sequence) > 0
    assert client.get_final_sequence()[0].item() == 'Hello'


def test_protocol_client_server_benign(two_channels):
    server, client = two_channels

    # Create a server request for a command
    client.sequence_command_local(SampleCommand('Hello'))
    msg_list = client.tap()
    assert len(msg_list) == 1
    request = msg_list.pop()
    assert isinstance(request, CommandRequestObject)
    assert client.other_next_seq() == 0
    assert client.my_next_seq() == 1

    # Send to server
    assert server.other_next_seq() == 0
    reply = server.handle_request(request)
    # msg_list = server.tap()
    # assert len(msg_list) == 1
    # reply = msg_list.pop()
    assert isinstance(reply, CommandResponseObject)
    assert server.other_next_seq() == 1
    assert server.next_final_sequence() == 1
    assert len(server.executor.command_status_sequence) > 0

    # Pass response back to client
    assert client.my_requests[0].response is None
    client.handle_response(reply)
    msg_list = client.tap()
    assert len(msg_list) == 0  # No message expected

    assert len(client.executor.command_status_sequence) > 0
    assert client.my_requests[0].response is not None
    assert client.get_final_sequence()[0].item() == 'Hello'
    assert client.next_final_sequence() == 1
    assert client.my_next_seq() == 1
    assert server.my_next_seq() == 0


def test_protocol_server_client_interleaved_benign(two_channels):
    server, client = two_channels

    client.sequence_command_local(SampleCommand('Hello'))
    client_request = client.tap()[0]
    server.sequence_command_local(SampleCommand('World'))
    server_request = server.tap()[0]

    # The server waits until all own requests are done
    server_reply = server.handle_request(client_request)
    # server_reply = server.tap()[0]
    assert server_reply.error.code == 'wait'

    client_reply = client.handle_request(server_request)
    server.handle_response(client_reply)
    server_reply = server.handle_request(client_request)

    client.handle_response(server_reply)

    assert len(client.my_requests) == 1
    assert len(server.other_requests) == 1
    assert len(client.get_final_sequence()) == 2
    assert len(server.get_final_sequence()) == 2
    assert [c.item() for c in client.get_final_sequence()] == [
        'World', 'Hello']
    assert [c.item() for c in server.get_final_sequence()] == [
        'World', 'Hello']


def test_protocol_server_client_interleaved_swapped_request(two_channels):
    server, client = two_channels

    client.sequence_command_local(SampleCommand('Hello'))
    client_request = client.tap()[0]
    server.sequence_command_local(SampleCommand('World'))
    server_request = server.tap()[0]

    client_reply = client.handle_request(server_request)
    server_reply = server.handle_request(client_request)
    assert server_reply.error.code == 'wait'

    server.handle_response(client_reply)
    server_reply = server.handle_request(client_request)

    client.handle_response(server_reply)

    assert len(client.my_requests) == 1
    assert len(server.other_requests) == 1
    assert len(client.get_final_sequence()) == 2
    assert len(server.get_final_sequence()) == 2
    assert [c.item() for c in client.get_final_sequence()] == [
        'World', 'Hello']
    assert [c.item() for c in server.get_final_sequence()] == [
        'World', 'Hello']


def test_protocol_server_client_interleaved_swapped_reply(two_channels):
    server, client = two_channels

    client.sequence_command_local(SampleCommand('Hello'))
    client_request = client.tap()[0]
    server.sequence_command_local(SampleCommand('World'))
    server_request = server.tap()[0]

    server_reply = server.handle_request(client_request)
    # server_reply = server.tap()[0]
    assert server_reply.error.code == 'wait'

    client_reply = client.handle_request(server_request)
    # client_reply = client.tap()[0]

    server.handle_response(client_reply)
    server_reply = server.handle_request(client_request)

    client.handle_response(server_reply)

    assert len(client.my_requests) == 1
    assert len(server.other_requests) == 1
    assert len(client.get_final_sequence()) == 2
    assert len(server.get_final_sequence()) == 2
    assert [c.item() for c in client.get_final_sequence()] == [
        'World', 'Hello']
    assert [c.item() for c in server.get_final_sequence()] == [
        'World', 'Hello']


def test_random_interleave_no_drop(two_channels):
    server, client = two_channels

    NUMBER = 20
    commands = list(range(NUMBER))
    commands = [SampleCommand(c) for c in commands]

    R = RandomRun(server, client, commands, seed='drop')
    R.DROP = False
    R.run()

    R.checks(NUMBER)

    client = R.client
    server = R.server

    # Print stats:
    print()
    print("Client: Requests #%d  Responses #%d" %
          (client.xx_requests_stats, client.xx_replies_stats))
    print("Server: Requests #%d  Responses #%d" %
          (server.xx_requests_stats, server.xx_replies_stats))


def test_random_interleave_and_drop(two_channels):
    server, client = two_channels

    NUMBER = 20
    commands = list(range(NUMBER))
    commands = [SampleCommand(c) for c in commands]

    R = RandomRun(server, client, commands, seed='drop')
    R.run()
    R.checks(NUMBER)

    client = R.client
    server = R.server

    # Print stats:
    print()
    print("Client: Requests #%d  Responses #%d" %
          (client.xx_requests_stats, client.xx_replies_stats))
    print("Server: Requests #%d  Responses #%d" %
          (server.xx_requests_stats, server.xx_replies_stats))


def test_random_interleave_and_drop_and_invalid(two_channels):
    server, client = two_channels

    NUMBER = 20
    commands = list(range(NUMBER))
    commands = [SampleCommand(c) for c in commands]
    for c in commands:
        c.always_happy = False

    R = RandomRun(server, client, commands, seed='drop')
    R.run()
    R.checks(NUMBER)

    client = R.client
    server = R.server

    client_seq = [c.item() for c in client.get_final_sequence()]
    server_seq = [c.item() for c in server.get_final_sequence()]

    # Print stats:
    print()
    print("Client: Requests #%d  Responses #%d" %
          (client.xx_requests_stats, client.xx_replies_stats))
    print("Server: Requests #%d  Responses #%d" %
          (server.xx_requests_stats, server.xx_replies_stats))

    server_store_keys = server.executor.object_liveness.keys()
    client_store_keys = client.executor.object_liveness.keys()
    assert set(server_store_keys) == set(client_store_keys)


def test_dependencies(two_channels):
    server, client = two_channels

    # Commands with dependencies
    cmd = [(0, []),
           (1, [0]),
           (2, []),
           (3, []),
           (4, [0]),
           (5, []),
           (6, [2]),
           (7, []),
           (8, [1]),
           (9, [4]),
           ]

    NUMBER = len(cmd)
    commands = [SampleCommand(c, deps) for c, deps in cmd]

    R = RandomRun(server, client, commands, seed='deps')
    R.run()
    R.checks(NUMBER)

    client = R.client
    server = R.server

    command_status_sequence = client.executor.command_status_sequence
    mapcmd = {
        c.item(): command_status_sequence[i] for i, c in enumerate(
            client.get_final_sequence()
        )
    }
    # Only one of the items with common dependency commits
    assert sum([mapcmd[1], mapcmd[4]]) == 1
    assert sum([mapcmd[8], mapcmd[9]]) == 1
    # All items commit (except those with common deps)
    assert sum(mapcmd.values()) == 8


def test_json_serlialize():
    # Test Commands (to ensure correct debug)
    cmd = SampleCommand(1, [2, 3])
    cmd2 = SampleCommand(10, [2, 3])
    data = cmd.get_json_data_dict(JSONFlag.NET)
    cmd2 = SampleCommand.from_json_data_dict(data, JSONFlag.NET)
    assert cmd == cmd2

    # Test Request, Response
    req0 = CommandRequestObject(cmd)
    req2 = CommandRequestObject(cmd2)
    req0.seq = 10
    req0.command_seq = 15
    req0.status = 'success'

    data = req0.get_json_data_dict(JSONFlag.STORE)
    assert data is not None
    req1 = CommandRequestObject.from_json_data_dict(data, JSONFlag.STORE)
    assert req0 == req1
    assert req1 != req2

    req0.response = make_protocol_error(req0, 'The error code')
    data_err = req0.get_json_data_dict(JSONFlag.STORE)
    assert data_err is not None
    assert data_err['response'] is not None
    req_err = CommandRequestObject.from_json_data_dict(
        data_err, JSONFlag.STORE)
    assert req0 == req_err


def test_VASProot(three_addresses, vasp):
    a0, a1, a2 = three_addresses

    # Check our own address is good
    assert vasp.get_vasp_address() == a0
    # Calling twice gives the same instance (use 'is')
    assert vasp.get_channel(a1) is vasp.get_channel(a1)
    # Different VASPs have different objects
    assert vasp.get_channel(a1) is not vasp.get_channel(a2)
    assert vasp.get_channel(a2).is_client()


def test_VASProot_diff_object(vasp, three_addresses):
    a0, _, b1 = three_addresses
    b2 = deepcopy(b1)

    # Check our own address is good
    assert vasp.get_vasp_address() == a0
    # Calling twice gives the same instance (use 'is')
    assert vasp.get_channel(b1) is vasp.get_channel(b2)


def test_real_address(three_addresses):
    from os import urandom
    A, _, B = three_addresses
    Ap = deepcopy(A)
    assert B.greater_than_or_equal(A)
    assert not A.greater_than_or_equal(B)
    assert A.greater_than_or_equal(A)
    assert A.greater_than_or_equal(Ap)
    assert A.equal(A)
    assert A.equal(Ap)
    assert not A.equal(B)
    assert not B.equal(Ap)
    assert A.last_bit() ^ B.last_bit() == 1
    assert A.last_bit() ^ A.last_bit() == 0


def test_sample_command():
    store = {}
    cmd1 = SampleCommand('hello')
    store['hello'] = cmd1.get_object('hello', store)
    cmd2 = SampleCommand('World', deps=['hello'])
    obj = cmd2.get_object('World', store)

    data = obj.get_json_data_dict(JSONFlag.STORE)
    obj2 = JSONSerializable.parse(data, JSONFlag.STORE)
    assert obj2.version == obj.version
    assert obj2.previous_versions == obj.previous_versions


def test_parse_handle_request_to_future(signed_json_request, channel, key):
    loop = asyncio.new_event_loop()
    fut = channel.parse_handle_request_to_future(
        signed_json_request, loop=loop)
    res = fut.result().content
    res = json.loads(key.verify_message(res))
    assert res['status'] == 'success'


def test_parse_handle_request_to_future_out_of_order(json_request, channel,
                                                     key):
    json_request['seq'] = 100
    json_request = key.sign_message(json.dumps(json_request))
    loop = asyncio.new_event_loop()
    fut = channel.parse_handle_request_to_future(
        json_request, loop=loop, nowait=True
    )
    res = fut.result().content
    res = json.loads(key.verify_message(res))
    assert res['error']['code'] == 'wait'


def test_parse_handle_request_to_future_parsing_error(json_request, channel,
                                                      key):
    json_request['seq'] = '"'  # Trigger a parsing error.
    json_request = key.sign_message(json.dumps(json_request))
    loop = asyncio.new_event_loop()
    fut = channel.parse_handle_request_to_future(json_request, loop=loop)
    res = fut.result().content
    res = json.loads(key.verify_message(res))
    assert res['error']['code'] == 'parsing'


def test_parse_handle_request_to_future_exception(json_request, channel):
    loop = asyncio.new_event_loop()
    fut = channel.parse_handle_request_to_future(
        json_request, loop=loop  # json_request is not signed.
    )
    assert fut.exception() is not None


def test_handle_request_malformed(json_request, two_channels):
    channel, _ = two_channels
    request = CommandRequestObject.from_json_data_dict(
        json_request, JSONFlag.NET
    )
    request.command_seq = 1  # Trigger error.
    res = channel.handle_request(request)
    json_response = res.get_json_data_dict(JSONFlag.NET)
    assert json_response['error']['code'] == 'malformed'


def test_parse_handle_response_to_future(signed_json_response, channel,
                                         command):
    _ = channel.sequence_command_local(command)
    loop = asyncio.new_event_loop()
    fut = channel.parse_handle_response_to_future(
        signed_json_response, loop=loop
    )
    res = fut.result()
    assert res


def test_parse_handle_response_to_future_parsing_error(json_response, channel,
                                                       command, key):
    _ = channel.sequence_command_local(command)
    loop = asyncio.new_event_loop()
    json_response['seq'] = '"'  # Trigger a parsing error.
    json_response = key.sign_message(json.dumps(json_response))
    fut = channel.parse_handle_response_to_future(json_response, loop=loop)
    assert fut.exception() is not None


def test_parse_handle_response_to_future_out_of_order(json_response, channel,
                                                      command, key):
    _ = channel.sequence_command_local(command)
    loop = asyncio.new_event_loop()
    json_response['command_seq'] = 100  # Trigger OffChainOutOfOrder.
    json_response = key.sign_message(json.dumps(json_response))
    fut = channel.parse_handle_response_to_future(json_response, loop=loop)
    assert not fut.done()


def test_parse_handle_response_to_future_out_of_order_with_nowait(json_response,
                                                                  channel,
                                                                  command, key):
    _ = channel.sequence_command_local(command)
    loop = asyncio.new_event_loop()
    json_response['command_seq'] = 100  # Trigger OffChainOutOfOrder.
    json_response = key.sign_message(json.dumps(json_response))
    fut = channel.parse_handle_response_to_future(
        json_response, loop=loop, nowait=True
    )
    assert fut.exception() is not None


def test_parse_handle_response_wrong_type(json_response, channel):
    response = CommandResponseObject.from_json_data_dict(
        json_response, JSONFlag.NET
    )
    response.seq = 'A'  # Raises OffChainException because it is not an int.
    with pytest.raises(OffChainException):
        channel.handle_response(response)


def test_parse_handle_response_bad_duplicate(json_response, channel, command):
    _ = channel.sequence_command_local(command)
    response = CommandResponseObject.from_json_data_dict(
        json_response, JSONFlag.NET
    )
    channel.handle_response(response)
    response.status = '1234'
    with pytest.raises(OffChainException):
        channel.handle_response(response)


def test_process_waiting_messages(channel, json_response, command, key):
    # Add command to the waiting queue.
    _ = channel.sequence_command_local(command)
    loop = asyncio.new_event_loop()
    json_response['command_seq'] = 100
    json_response = key.sign_message(json.dumps(json_response))
    fut = channel.parse_handle_response_to_future(json_response, loop=loop)
    assert not fut.done()

    # Clear the waiting queue.
    channel.executor = MagicMock(spec=ProtocolExecutor)
    channel.executor.next_seq.return_value = 100
    type(channel.executor).last_confirmed = PropertyMock(return_value=100)
    channel.process_waiting_messages()
    assert not channel.waiting_response


def test_handle_response_executor_exception(channel, signed_json_response,
                                            command):
    _ = channel.sequence_command_local(command)
    channel.executor = MagicMock(spec=ProtocolExecutor)
    channel.executor.sequence_next_command.side_effect = ExecutorException
    channel.executor.next_seq.return_value = 0
    type(channel.executor).last_confirmed = PropertyMock(return_value=0)
    loop = asyncio.new_event_loop()
    fut = channel.parse_handle_response_to_future(
        signed_json_response, loop=loop
    )
    assert fut.exception() is not None


def test_get_storage_factory(vasp):
    assert isinstance(vasp.get_storage_factory(), StorableFactory)


def test_role(channel):
    assert channel.role() == 'Client'


def test_num_pending_responses(channel):
    assert channel.num_pending_responses() == 0


def test_pending_retransmit_number(channel):
    assert channel.pending_retransmit_number() == 0
