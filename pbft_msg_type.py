from enum import Enum
import json


class MsgType(str, Enum):
    PREPAREMSG = 1
    COMMITMSG = 2
    INVOLVEPREPAREMSG = 3
    INVOLVECOMMITMSG = 4


class RequestMsg:  # 客户端发给服务器的请求request
    def __init__(self, timestamp: int, clientID: int, operation, sequenceID: int, ringOrder: list) -> None:
        self.Timestamp = timestamp
        self.ClientID = clientID
        self.Operation = operation
        self.SequenceID = sequenceID
        self.RingOrder = ringOrder


class InvolveRequestMsg:  # 客户端发给服务器的请求request
    def __init__(self, timestamp: int, nodeID: str, myPort: int, scheme) -> None:
        self.Timestamp = timestamp
        self.NodeID = nodeID
        self.Port = myPort
        self.Scheme = scheme


class ReplyMsg:  # 达成共识后各个分片执行操作后进行回应
    def __init__(self, viewID, timestamp, clientID, nodeID, result) -> None:
        self.ViewID = viewID
        self.Timestamp = timestamp
        self.ClientID = clientID
        self.NodeID = nodeID
        self.Result = result


class PrePrepareMsg:
    def __init__(self, viewID: int, sequenceID: int, digest, requestMsg) -> None:
        self.ViewID = viewID
        self.SequenceID = sequenceID
        self.Digest = digest
        self.RequestMsg = requestMsg


class VoteMsg:
    def __init__(self, viewID: int, sequenceID: int, digest, nodeID, msgType: MsgType, myvote: bool) -> None:
        self.ViewID = viewID
        self.SequenceID = sequenceID
        self.Digest = digest
        self.NodeID = nodeID
        self.MsgType = msgType
        self.Vote = myvote


class GlobalForwardMsg:
    def __init__(self, reqMsg: RequestMsg, voteMsg: VoteMsg, digest) -> None:
        self.ReqMsg = reqMsg
        self.CommitMsg = voteMsg
        self.Digest = digest


class LocalForwardMsg:
    def __init__(self, globalForwardMsg: GlobalForwardMsg) -> None:
        self.LocalForwardMsg = globalForwardMsg


def EncodeRequestMsg(reqMsg: RequestMsg):
    return reqMsg.__dict__


def EncodePrePrepareMsg(prePrepareMsg: PrePrepareMsg):
    # print(type(prePrepareMsg.RequestMsg),flush=True)
    t = prePrepareMsg.RequestMsg.__dict__
    prePrepareMsg.RequestMsg = t
    return prePrepareMsg.__dict__


def EncodeVoteMsg(msg: VoteMsg):
    return msg.__dict__


def EncodeReplyMsg(msg: ReplyMsg):
    return msg.__dict__


def EncodeGlobalForwardMsg(gfMsg: GlobalForwardMsg):
    if type(gfMsg.ReqMsg) == RequestMsg:
        tReqMsg = gfMsg.ReqMsg.__dict__
        tComMsg = gfMsg.CommitMsg.__dict__
        tGFMsg = gfMsg.__dict__
        tGFMsg['ReqMsg'] = tReqMsg
        tGFMsg['CommitMsg'] = tComMsg
        return tGFMsg
    elif type(gfMsg.ReqMsg) == dict:
        return gfMsg.__dict__


def EncodeLocalForwardMsg(lfMsg: LocalForwardMsg):
    t = lfMsg.LocalForwardMsg
    tLFMsg = lfMsg.__dict__
    tLFMsg["LocalForwardMsg"] = EncodeGlobalForwardMsg(t)
    return tLFMsg


def EncodeInvolveRequestMsg(msg: InvolveRequestMsg):
    return msg.__dict__


def DecodeRequestMsg(msg: dict):
    return RequestMsg(msg['Timestamp'], msg['ClientID'], msg['Operation'], msg['SequenceID'], list(msg['RingOrder']))


def DecodePrePrepareMsg(msg: dict):
    reqMsg = RequestMsg(msg['RequestMsg']['Timestamp'], msg['RequestMsg']['ClientID'], msg['RequestMsg']
                        ['Operation'], msg['RequestMsg']['SequenceID'], list(msg['RequestMsg']['RingOrder']))
    return PrePrepareMsg(msg['ViewID'], msg['SequenceID'], msg['Digest'], reqMsg)


def DecodeVoteMsg(msg: dict):
    return VoteMsg(msg['ViewID'], msg['SequenceID'], msg['Digest'], msg['NodeID'], msg['MsgType'], msg['Vote'])


def DecodeReplyMsg(msg: dict):
    return ReplyMsg(msg['ViewID'], msg['Timestamp'], msg['ClientID'], msg['NodeID'], msg['Result'])


def DecodeGlobalForwardMsg(msg: dict):
    tReqMsg = DecodeRequestMsg(msg['ReqMsg'])
    tComMsg = DecodeVoteMsg(msg['CommitMsg'])
    return GlobalForwardMsg(tReqMsg, tComMsg, msg['Digest'])


def DecodeLocalForwardMsg(msg: dict):
    return LocalForwardMsg(DecodeGlobalForwardMsg(msg["LocalForwardMsg"]))


def DecodeInvolveRequestMsg(msg: dict):
    return InvolveRequestMsg(msg['Timestamp'], msg['NodeID'], msg['Port'], msg['Scheme'])


if __name__ == "__main__":
    print("RingBFT start...")
    RequestMsg(1, 1, 'get name', 1, [1, 2])
