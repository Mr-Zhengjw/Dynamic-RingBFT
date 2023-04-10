from pbft_impl import *
from pbft_msg_type import *
from log import *
import json
import requests
import threading


ResolvingTimeDuration = 1


class View:
    def __init__(self, viewID: int, myPri: str) -> None:
        self.ID = viewID
        self.Primary = myPri


class MsgBuffer:
    def __init__(self, reqMsgs: list, prePrepareMsgs: list, prepareMsgs: list, commitMsgs: list, relpyMsgs: list, globalForwardMsgs: list, localForwardMsgs: list) -> None:
        self.ReqMsgs = reqMsgs
        self.PrePrepareMsgs = prePrepareMsgs
        self.PrepareMsgs = prepareMsgs
        self.CommitMsgs = commitMsgs
        self.ReplyMsgs = relpyMsgs
        self.GlobalForwardMsgs = globalForwardMsgs
        self.LocalForwardMsgs = localForwardMsgs


class Node:
    def __init__(self, nodeID, nodeTable: dict, view: View, currentState, committedMsgs: list, msgBuffer: MsgBuffer, msgEntrance: list, msgDelivery: list, lastConsensusMsgs: MsgLogs):
        self.NodeID = nodeID
        self.NodeTable = nodeTable
        self.View = view
        self.CurrentState = currentState
        self.CommittedMsgs = committedMsgs
        self.MsgBuffer = msgBuffer
        self.MsgEntrance = msgEntrance
        self.MsgDelivery = msgDelivery
        self.LastConsensusMsgs = lastConsensusMsgs

    def CreateStateForNewConsensus(self):
        if self.CurrentState != None:
            print('[PREPREPARE] Another consensus is ongoing', flush=True)
            return False
        lastSequenceID = -2
        if len(self.CommittedMsgs) == 0:
            lastSequenceID = -1
        else:
            lastSequenceID = self.CommittedMsgs[len(
                self.CommittedMsgs)-1].SequenceID
        if lastSequenceID != -2:
            self.CurrentState = CreatState(self.View.ID, lastSequenceID)
        LogStage("Create the replica status", True)
        return True

    def Reply(self, replyMsg: ReplyMsg):
        for comMsg in self.CommittedMsgs:
            # print(type(comMsg),flush=True)
            print("Committed value: {},{},{},{}".format(comMsg.ClientID,
                  comMsg.Timestamp, comMsg.Operation, comMsg.SequenceID), flush=True)
            print("http://" + self.NodeTable[self.View.Primary] +
                  '/reply :' + self.NodeID, flush=True)
        requests.post("http://" + self.NodeTable[self.View.Primary] +
                      '/reply', json=json.dumps(EncodeReplyMsg(replyMsg)))

    # primary 收到request后转发给其他节点
    def GetReq(self, reqMsg: RequestMsg):
        LogMsg(reqMsg)
        self.CreateStateForNewConsensus()
        if self.CurrentState is not None:
            prePrepareMsg = self.CurrentState.StartConsensus(reqMsg)
            LogStage("Consensus Process (ViewID:{})".format(
                self.CurrentState.ViewID), False)
            if prePrepareMsg is not None:
                LogStage("Pre-prepare", True)
                self.Broadcast(EncodePrePrepareMsg(
                    prePrepareMsg), '/preprepare')
                LogStage("Prepare", False)
                preMsg = VoteMsg(self.CurrentState.ViewID, prePrepareMsg.SequenceID,
                                 prePrepareMsg.Digest, self.NodeID, MsgType.PREPAREMSG, True)
                self.CurrentState.MsgLogs.PrepareMsgs[self.NodeID] = preMsg

    # 节点收到preprepare消息后，开启自己的preprepare阶段，即转发给其他节点
    def GetPrePrepare(self, prePrepareMsg: PrePrepareMsg):
        LogMsg(prePrepareMsg)
        if self.CurrentState is None:
            # LogMsg(prePrepareMsg.RequestMsg)
            # print(getDigest(prePrepareMsg.RequestMsg))
            # LogMsg(prePrepareMsg.Digest)
            self.CreateStateForNewConsensus()

        if self.CurrentState is not None:
            self.CurrentState.MsgLogs.ReqMsg = prePrepareMsg.RequestMsg

            # LogMsg(prePrepareMsg.RequestMsg)
            # print('PrePrepare {}'.format(getDigest(prePrepareMsg.RequestMsg)),flush=True)

            prepareMsg = self.CurrentState.PrePrepare(
                self.NodeID, prePrepareMsg)
            if prepareMsg is not None:
                self.CurrentState.MsgLogs.PrepareMsgs[self.NodeID] = prepareMsg
                print("{} GetPrePrepare prepareMsg {}".format(
                    self.NodeID, prepareMsg.Digest), flush=True)
                LogStage("Pre-prepare", True)
                self.Broadcast(EncodeVoteMsg(prepareMsg), "/prepare")
                LogStage("Prepare", False)

    def GetPrepare(self, prepareMsg: VoteMsg):
        # print(" GetPrepare",flush=True)
        LogMsg(prepareMsg)
        if self.CurrentState is not None:
            (commitMsg, preResult) = self.CurrentState.Prepare(
                self.NodeID, prepareMsg)
            if preResult == False:
                print("[As prepare votes are FALSE, \'{}\' will not be executed] ".format(
                    self.CurrentState.MsgLogs.ReqMsg.Operation), flush=True)
                self.RestartNode()
            else:
                if commitMsg is not None:
                    # 测试，拜占庭节点投相反的票
                    # if self.NodeID[-1] == "3" or self.NodeID[-1] == "2" or self.NodeID[-1] == "1":
                    #     commitMsg.Vote = False
                    LogStage("Prepare", True)
                    self.Broadcast(EncodeVoteMsg(commitMsg), "/commit")
                    LogStage("Commit", False)

    def GetCommit(self, commitMsg):
        LogMsg(commitMsg)
        if self.CurrentState is not None:
            (replyMsg, commitedMsg, globalForwardMsg,
             comResult) = self.CurrentState.Commit(self.NodeID, commitMsg)
            if replyMsg is not None and comResult == False:
                print("[Execute NO] As commit votes are FALSE, \'{}\' will not be executed".format(
                    self.CurrentState.MsgLogs.ReqMsg.Operation), flush=True)
                self.getExecute(
                    self.CurrentState.MsgLogs.ReqMsg.Operation, False)
                self.RestartNode()
            else:
                if replyMsg is not None:
                    self.getExecute(
                        self.CurrentState.MsgLogs.ReqMsg.Operation, True)
                    LogStage("Commit", True)
                    self.Reply(replyMsg)
                    LogStage("Reply", True)
                    print("[Execute Transactions Once]", flush=True)
                if globalForwardMsg is not None:
                    LogStage("Global Sharing", False)
                    self.GlobalShare(globalForwardMsg, "GetCommit")
                    LogStage("Global Sharing", True)

    def GetReply(self, replyMsg: ReplyMsg):
        # print("GetReply", flush=True)
        # print("Result of {} by {} : {}".format(replyMsg.Result, replyMsg.NodeID, replyMsg.Result),flush=True)
        if self.CurrentState is not None and self.CurrentState.CurrentStage == stage.INVOLVINGCOMMITED:
            self.CurrentState.MsgLogs.CommitMsgs[self.NodeID] = self.LastConsensusMsgs.CommitMsgs[self.NodeID]
            invReqMsg = self.CurrentState.MsgLogs.ReqMsg
            self.CurrentState.MsgLogs.ReqMsg = RequestMsg(
                invReqMsg.Timestamp, 5, "InvolveRequest " + invReqMsg.NodeID + " " + str(invReqMsg.Port), 24, [[1, 2, 3], [1, 1, 1]])
            self.getExecute("InvolveRequest " + invReqMsg.NodeID +
                            " " + str(invReqMsg.Port), True)
            self.CurrentState.MsgLogs.ReqMsg = invReqMsg
        elif self.CurrentState is not None and self.CurrentState.MsgLogs.ReqMsg.Timestamp == replyMsg.Timestamp and self.NodeID != self.CurrentState.MsgLogs.ReqMsg.Operation.split(' ')[1]:
            reqPort = self.CurrentState.MsgLogs.ReqMsg.Operation.split(' ')[2]
            requests.post("http://127.0.0.1:" + reqPort + "/reply",
                          json=json.dumps(EncodeReplyMsg(replyMsg)))
        elif self.LastConsensusMsgs.ReqMsg is not None and self.LastConsensusMsgs.ReqMsg.Timestamp == replyMsg.Timestamp and self.LastConsensusMsgs.ReqMsg.Operation.split(' ')[0] == "InvolveRequest" and self.NodeID != self.LastConsensusMsgs.ReqMsg.Operation.split(' ')[1]:
            reqPort = self.LastConsensusMsgs.ReqMsg.Operation.split(' ')[2]
            requests.post("http://127.0.0.1:" + reqPort + "/reply",
                          json=json.dumps(EncodeReplyMsg(replyMsg)))

    def GetGlobalForward(self, globalForwardMsg: GlobalForwardMsg):
        LogMsg(globalForwardMsg.ReqMsg)
        # print("{} {} {}".format(type(globalForwardMsg.ReqMsg),
        #                         type(globalForwardMsg.CommitMsg), globalForwardMsg.CommitMsg.NodeID), flush=True)
        if self.CurrentState is not None and self.CurrentState.CurrentStage == stage.EXECUTED:
            return None
        if self.CurrentState is not None and verifyForward(globalForwardMsg) == False:
            return None
        LogMsg(globalForwardMsg)
        self.MsgBuffer.GlobalForwardMsgs.append(globalForwardMsg)
        localForwardMsg = LocalForwardMsg(globalForwardMsg)
        if globalForwardMsg.ReqMsg.Operation.split(" ")[0] == "InvolveRequest":
            if self.CurrentState is None:
                self.CreateStateForNewConsensus()
            if self.CurrentState is not None:
                if self.CurrentState.CurrentStage != stage.INVOLVINGEXECUTED:
                    self.CurrentState.CurrentStage = stage.INVOLVINGCOMMITED
                self.CurrentState.MsgLogs.GlobalForwardMsgs[globalForwardMsg.CommitMsg.NodeID] = globalForwardMsg
                # print("{} {} {}".format(type(self.CurrentState.MsgLogs.GlobalForwardMsgs[globalForwardMsg.CommitMsg.NodeID].ReqMsg),
                #                         type(self.CurrentState.MsgLogs.GlobalForwardMsgs[globalForwardMsg.CommitMsg.NodeID].CommitMsg), self.CurrentState.MsgLogs.GlobalForwardMsgs[globalForwardMsg.CommitMsg.NodeID].CommitMsg.NodeID), flush=True)
        LogStage("Local Sharing", False)
        self.Broadcast(EncodeLocalForwardMsg(localForwardMsg), "/localforward")
        LogStage("Local Sharing", True)

    def GetLocalForward(self, localForwardMsg: LocalForwardMsg):
        globalForwardMsg = localForwardMsg.LocalForwardMsg

        # print("GetLocalForward : {}".format(
        #     type(self.MsgBuffer.GlobalForwardMsgs[0].ReqMsg)), flush=True)
        if self.NodeID[-1] == '0':
            if self.CurrentState is None:
                self.CreateStateForNewConsensus()
                if self.CurrentState is not None:
                    self.CurrentState.MsgLogs.GlobalForwardMsgs[
                        globalForwardMsg.CommitMsg.NodeID] = globalForwardMsg
            elif self.CurrentState.CurrentStage == stage.COMMITED:
                # 当收到全局转发的事务后，本地确认之后，如果本地阶段进行到了COMMITED阶段，那么说明已经收到了第一轮中欠缺的信息
                print("[Execute Transaction Twice] {}".format(
                    self.CurrentState.MsgLogs.ReqMsg.Operation), flush=True)
                self.CurrentState.CurrentStage = stage.EXECUTED
                self.GlobalShare(globalForwardMsg, "GetLocalForward")
                self.RestartNode()
            elif self.CurrentState.CurrentStage == stage.EXECUTED:
                pass
            else:
                self.CurrentState.MsgLogs.GlobalForwardMsgs[globalForwardMsg.CommitMsg.NodeID] = globalForwardMsg
                if len(self.CurrentState.MsgLogs.GlobalForwardMsgs) >= 2*f and self.CurrentState.CurrentStage != stage.PREPREPARED:
                    LogMsg(globalForwardMsg.ReqMsg)
                    # print("GetLocalForward 3else {}".format(getDigest(globalForwardMsg.ReqMsg)),flush=True)
                    prePrepareMsg = self.CurrentState.StartConsensus(
                        globalForwardMsg.ReqMsg)
                    # self.CurrentState.CurrentStage = stage.INITIAL
                    if prePrepareMsg is not None:
                        # print("GetLocalForward prePrepareMsg: {} {}".format(prePrepareMsg.Digest,getDigest(prePrepareMsg.RequestMsg)),flush=True)
                        self.CurrentState.MsgLogs.ReqMsg = globalForwardMsg.ReqMsg
                        self.Broadcast(EncodePrePrepareMsg(
                            prePrepareMsg), "/preprepare")
                        LogStage("Pre-prepare", True)
        else:
            if self.CurrentState is not None and self.CurrentState.CurrentStage == stage.COMMITED:
                # 当收到全局转发的事务后，本地确认之后，如果本地阶段进行到了COMMITED阶段，那么说明已经收到了第一轮中欠缺的信息
                print("[Execute Transaction Twice] {}".format(
                    self.CurrentState.MsgLogs.ReqMsg), flush=True)
                self.CurrentState.CurrentStage = stage.EXECUTED
                self.GlobalShare(globalForwardMsg, "GetLocalForward")
                self.RestartNode()
            else:
                pass

    def GetInvolve(self, invReqMsg: InvolveRequestMsg):
        if invReqMsg.Scheme == 0:
            self.NodeTable[invReqMsg.NodeID] = "127.0.0.1:" + \
                str(invReqMsg.Port)
            self.CurrentState
            if self.NodeID == self.View.Primary:
                LogStage("InvolveForward", False)
                self.Broadcast(EncodeInvolveRequestMsg(
                    invReqMsg), "/involveReq")
                LogStage("InvolveForward", True)
                msg = RequestMsg(invReqMsg.Timestamp, 5, "InvolveRequest " + invReqMsg.NodeID + " " + str(invReqMsg.Port), 24, [
                    [1, 2, 3], [1, 1, 1]])
                requests.post(
                    "http://" + self.NodeTable[self.NodeID] + "/req", json=json.dumps(EncodeRequestMsg(msg)))

        elif invReqMsg.Scheme == 1:
            if self.NodeID == self.View.Primary:
                LogStage("InvolveForward", False)
                self.Broadcast(EncodeInvolveRequestMsg(
                    invReqMsg), "/involveReq")
                LogStage("InvolveForward", True)
                msg = RequestMsg(invReqMsg.Timestamp, 5, "InvolveRequest " + invReqMsg.NodeID + " " + str(invReqMsg.Port), 24, [
                                 [1, 2, 3], [1, 1, 1]])
                requests.post(
                    "http://" + self.NodeTable[self.NodeID] + "/req", json=json.dumps(EncodeRequestMsg(msg)))

        elif invReqMsg.Scheme == 2:
            self.CreateStateForNewConsensus()
            if self.CurrentState is not None:
                self.CurrentState.CurrentStage = stage.INVOLVINGINITIAL
                self.CurrentState.MsgLogs.ReqMsg = invReqMsg
            if self.NodeID == invReqMsg.NodeID:
                LogStage("InvolveForward", False)
                self.Broadcast(EncodeInvolveRequestMsg(
                    invReqMsg), "/involveReq")
                LogStage("InvolveForward", True)
                # print(self.CurrentState, flush=True)
            else:
                # 副本检验收到的消息后
                self.NodeTable[invReqMsg.NodeID] = "127.0.0.1:" + \
                    str(invReqMsg.Port)
                requests.post("http://" + self.NodeTable[invReqMsg.NodeID] + "/hisInvolveReq", json=json.dumps(
                    EncodeRequestMsg(self.LastConsensusMsgs.ReqMsg)))
                if self.CurrentState is not None:
                    self.CurrentState.CurrentStage = stage.INVOLVINGPREPREPARED

    def GetInvolveReq(self, LastReqMsg: RequestMsg):
        if self.CurrentState is not None and self.NodeID == self.CurrentState.MsgLogs.ReqMsg.NodeID:
            # print(getDigest(LastReqMsg),flush=True)
            self.CurrentState.MsgLogs.InvReqMsgs.append(LastReqMsg)
            LogStage("Involve Pre-prepare", False)
            prepareMsg = self.CurrentState.InvolveVerifyReq(
                self.NodeID, LastReqMsg)
            if prepareMsg is not None:
                self.CurrentState.MsgLogs.PrepareMsgs[self.NodeID] = prepareMsg
                LogStage("Involve Pre-prepare", True)
                self.Broadcast(EncodeVoteMsg(prepareMsg), "/hisInvolvePrepare")
                LogStage("Involve Prepare", False)
                print(self.CurrentState.CurrentStage, flush=True)

    def GetInvolvePrepare(self, prepareMsg: VoteMsg):
        if self.CurrentState is not None:
            # print(self.CurrentState.MsgLogs.ReqMsg.NodeID,flush=True)
            if self.NodeID == self.CurrentState.MsgLogs.ReqMsg.NodeID:
                # print('InvolveVerifyPrepare if {} msg by {}'.format(self.CurrentState.CurrentStage,prepareMsg.NodeID),flush=True)
                # prepareMsg.MsgType = MsgType.PREPAREMSG
                # (commitMsg, preResult) = self.CurrentState.Prepare(self.NodeID, prepareMsg)\
                if self.CurrentState.InvolveVerifyPrepare(prepareMsg, self.CurrentState.MsgLogs.InvReqMsgs[0], self.CurrentState.MsgLogs.PrepareMsgs):
                    invReq = self.CurrentState.MsgLogs.ReqMsg
                    self.CurrentState.MsgLogs.ReqMsg = self.CurrentState.MsgLogs.InvReqMsgs[0]
                    (commitMsg, preResult) = self.CurrentState.Prepare(
                        self.NodeID, prepareMsg)
                    self.CurrentState.MsgLogs.ReqMsg = invReq
                    self.CurrentState.CurrentStage = stage.INVOLVINGPREPARED
                    if preResult == False:
                        print(
                            "[As prepare votes are FALSE, Invovle operation will not be executed]", flush=True)
                        self.RestartNode()
                    else:
                        if commitMsg is not None:
                            # 测试，拜占庭节点投相反的票
                            # if self.NodeID[-1] == "3" or self.NodeID[-1] == "2" or self.NodeID[-1] == "1":
                            #     commitMsg.Vote = False
                            commitMsg.MsgType = MsgType.INVOLVECOMMITMSG
                            print('InvolveVerifyPrepare if {} msg by {}\n'.format(
                                self.CurrentState.CurrentStage, prepareMsg.NodeID), flush=True)
                            LogStage("Involve Prepare", True)
                            self.Broadcast(EncodeVoteMsg(
                                commitMsg), "/hisInvolveCommit")
                            LogStage("Involve Commit", False)
                        else:
                            self.CurrentState.CurrentStage = stage.INVOLVINGPREPREPARED
            else:
                if self.CurrentState.InvolveVerifyPrepare(prepareMsg, self.LastConsensusMsgs.ReqMsg, self.LastConsensusMsgs.PrepareMsgs):
                    # print('InvolveVerifyPrepare',flush=True)
                    lastPrepareMsg = self.LastConsensusMsgs.PrepareMsgs[self.NodeID]
                    lastPrepareMsg.MsgType = MsgType.INVOLVEPREPAREMSG
                    # 测试，拜占庭节点投相反的票
                    # if self.NodeID[-1] == "3" or self.NodeID[-1] == "2":
                    #     # commitMsg.Vote = False
                    #     return
                    requests.post("http://" + self.NodeTable[self.CurrentState.MsgLogs.ReqMsg.NodeID] +
                                  "/hisInvolvePrepare", json=json.dumps(EncodeVoteMsg(lastPrepareMsg)))
                    print('[Send] ' + self.NodeTable[self.CurrentState.MsgLogs.ReqMsg.NodeID] +
                          "/hisInvolvePrepare", flush=True)
                    self.CurrentState.CurrentStage = stage.INVOLVINGPREPARED

    def GetInvolveCommit(self, commitMsg: VoteMsg):
        if self.CurrentState is not None:
            if self.NodeID == self.CurrentState.MsgLogs.ReqMsg.NodeID:
                # print('InvolveVerifyCommit if {} msg by {}'.format(
                #     self.CurrentState.CurrentStage, commitMsg.NodeID), flush=True)
                if self.CurrentState.InvolveVerifyCommit(commitMsg, self.CurrentState.MsgLogs.InvReqMsgs[0], self.CurrentState.MsgLogs.CommitMsgs):
                    invReq = self.CurrentState.MsgLogs.ReqMsg
                    self.CurrentState.MsgLogs.ReqMsg = self.CurrentState.MsgLogs.InvReqMsgs[0]
                    # (commitMsg, preResult) = self.CurrentState.Prepare(self.NodeID, prepareMsg)
                    (replyMsg, commitedMsg, globalForwardMsg,
                     comResult) = self.CurrentState.Commit(self.NodeID, commitMsg)
                    self.CurrentState.MsgLogs.ReqMsg = invReq
                    self.CurrentState.CurrentStage = stage.INVOLVINGCOMMITED
                    if comResult == False:
                        print(
                            "[As prepare votes are FALSE, Invovle operation will not be executed]", flush=True)
                        self.RestartNode()
                    else:
                        if replyMsg is not None:
                            # 测试，拜占庭节点投相反的票
                            # if self.NodeID[-1] == "3" or self.NodeID[-1] == "2" or self.NodeID[-1] == "1":
                            #     commitMsg.Vote = False
                            LogStage("Involve Commit", True)
                            self.Broadcast(EncodeReplyMsg(replyMsg), "/reply")
                            invReq = self.CurrentState.MsgLogs.ReqMsg
                            self.CurrentState.MsgLogs.ReqMsg = RequestMsg(invReq.Timestamp, 5, "InvolveRequest r1_4 30004", 24, [
                                [1, 2, 3], [1, 1, 1]])
                            self.getExecute(
                                "InvolveRequest " + invReq.NodeID + " " + str(invReq.Port), True)
                            self.CurrentState.MsgLogs.ReqMsg = invReq
                            # LogStage("Involve Commit", False)
                        else:
                            self.CurrentState.CurrentStage = stage.INVOLVINGPREPARED
            else:
                if self.CurrentState.InvolveVerifyCommit(commitMsg, self.LastConsensusMsgs.ReqMsg, self.LastConsensusMsgs.CommitMsgs):
                    # print('InvolveVerifyCommit', flush=True)
                    lastComMsg = self.LastConsensusMsgs.CommitMsgs[self.NodeID]
                    lastComMsg.MsgType = MsgType.INVOLVECOMMITMSG
                    requests.post("http://" + self.NodeTable[self.CurrentState.MsgLogs.ReqMsg.NodeID] +
                                  "/hisInvolveCommit", json=json.dumps(EncodeVoteMsg(lastComMsg)))
                    print('[Send] ' + self.NodeTable[self.CurrentState.MsgLogs.ReqMsg.NodeID] +
                          "/hisInvolveCommit", flush=True)
                    self.CurrentState.CurrentStage = stage.INVOLVINGCOMMITED

    def GetInvolveGlobalShare(self, localForwardMsg: LocalForwardMsg):
        globalForwardMsg = localForwardMsg.LocalForwardMsg

        # print("{} {}".format(type(self.CurrentState.MsgLogs.GlobalForwardMsgs[self.NodeID[0]+str(
        #     (int(self.NodeID[1])-2) % 3 + 1)+self.NodeID[-2:]].ReqMsg), self.NodeID[0]+str(
        #     (int(self.NodeID[1])-2) % 3 + 1)+self.NodeID[-2:]), flush=True)
        if self.CurrentState is not None:
            # print(len(self.CurrentState.MsgLogs.GlobalForwardMsgs), flush=True)
            if len(self.CurrentState.MsgLogs.GlobalForwardMsgs) <= 2*f:
                self.CurrentState.MsgLogs.GlobalForwardMsgs[globalForwardMsg.CommitMsg.NodeID] = globalForwardMsg
            else:
                if globalForwardMsg.ReqMsg.Operation.split(" ")[1] not in self.NodeTable.keys():
                    # self.NodeTable[globalForwardMsg.ReqMsg.Operation.split(
                    #     " ")[1]] = "127.0.0.1:" + globalForwardMsg.ReqMsg.Operation.split(" ")[2]
                    # print("")
                    msg = self.CurrentState.MsgLogs.GlobalForwardMsgs[self.NodeID[0]+str(
                        (int(self.NodeID[1])-2) % 3 + 1)+self.NodeID[-2:]]
                    msg.ReqMsg = DecodeRequestMsg(msg.ReqMsg)
                    msg.CommitMsg = DecodeVoteMsg(msg.CommitMsg)
                    msg.CommitMsg.NodeID = self.NodeID
                    self.getExecute(globalForwardMsg.ReqMsg.Operation, True)
                    self.GlobalShare(msg, "InvolveForward")
                self.RestartNode()

    def DispatchMsg(self):
        while 1:
            if len(self.MsgEntrance) != 0:
                self.RouteMsg(self.MsgEntrance.pop(0))
                # time.sleep(1)

    def ResolveMsg(self):
        while 1:
            if len(self.MsgDelivery) != 0:
                # print("Resolve {} {}".format(
                #     type(self.MsgDelivery[0]), self.MsgDelivery[0]), flush=True)
                msg = self.MsgDelivery.pop(0)
                msgType = type(msg)
                if msgType == RequestMsg:
                    if self.CurrentState is not None and self.CurrentState.CurrentStage == stage.INVOLVINGINITIAL:
                        # print("self.CurrentState is not None and self.CurrentState.CurrentStage == stage.INVOLVING",flush=True)
                        self.GetInvolveReq(msg)
                    elif self.CurrentState is None:
                        self.GetReq(msg)
                elif msgType == InvolveRequestMsg:
                    self.GetInvolve(msg)
                elif msgType == PrePrepareMsg:
                    self.GetPrePrepare(msg)
                elif msgType == VoteMsg:
                    if msg.MsgType == MsgType.PREPAREMSG and self.CurrentState is not None and self.CurrentState.CurrentStage == stage.PREPREPARED:
                        self.GetPrepare(msg)
                    elif msg.MsgType == MsgType.COMMITMSG and self.CurrentState is not None and self.CurrentState.CurrentStage == stage.PREPARED:
                        self.GetCommit(msg)
                    elif msg.MsgType == MsgType.INVOLVEPREPAREMSG and self.CurrentState is not None and self.CurrentState.CurrentStage == stage.INVOLVINGPREPREPARED:
                        # elif msg.MsgType == MsgType.INVOLVEPREPAREMSG:
                        # print("Resolve msg.MsgType == MsgType.INVOLVEPREPAREMSG {}".format(msg.NodeID),flush=True)
                        self.GetInvolvePrepare(msg)
                    elif msg.MsgType == MsgType.INVOLVECOMMITMSG and self.CurrentState is not None and self.CurrentState.CurrentStage == stage.INVOLVINGPREPARED:
                        # print("Resolve msg.MsgType == MsgType.INVOLVECOMMITMSG {}".format(
                        # self.CurrentState.CurrentStage), flush=True)
                        self.GetInvolveCommit(msg)
                elif msgType == ReplyMsg:
                    # print("Result: {} by {} on {}".format(
                    #     msg.Result, msg.NodeID, msg.Timestamp), flush=True)
                    self.GetReply(msg)
                elif msgType == GlobalForwardMsg:
                    self.GetGlobalForward(msg)
                elif msgType == LocalForwardMsg:
                    if self.CurrentState is not None and self.CurrentState.CurrentStage == stage.INVOLVINGCOMMITED:
                        self.GetInvolveGlobalShare(msg)
                    else:
                        self.GetLocalForward(msg)

    def RouteMsg(self, msg):
        if msg is not None:
            msgType = type(msg)
            # print(msgType == ReplyMsg,flush=True)
            if msgType == RequestMsg:
                if self.CurrentState is None or (self.CurrentState is not None and self.CurrentState.CurrentStage == stage.INVOLVINGINITIAL):
                    self.MsgBuffer.ReqMsgs.append(msg)
                    self.MsgDelivery.extend(self.MsgBuffer.ReqMsgs)
                    self.MsgBuffer.ReqMsgs = []
                    # print('self.CurrentState is None or (self.CurrentState is not None and self.CurrentState.CurrentStage == stage.INVOLVING',flush=True)
                else:
                    self.MsgBuffer.ReqMsgs.append(msg)
            elif msgType == InvolveRequestMsg:
                self.MsgDelivery.append(msg)
            elif msgType == PrePrepareMsg:
                # if self.CurrentState is None or self.CurrentState.CurrentStage == stage.INITIAL:
                if self.CurrentState is None:
                    self.MsgBuffer.PrePrepareMsgs.append(msg)
                    self.MsgDelivery.extend(self.MsgBuffer.PrePrepareMsgs)
                    self.MsgBuffer.PrePrepareMsgs = []
                else:
                    self.MsgBuffer.PrePrepareMsgs.append(msg)
            elif msgType == VoteMsg:
                # print("RouteMsg msgType == VoteMsg {} {}".format(self.CurrentState.CurrentStage,len(self.MsgBuffer.PrepareMsgs)), flush=True)
                if msg.MsgType == MsgType.PREPAREMSG:
                    if (self.CurrentState is None) or self.CurrentState.CurrentStage != stage.PREPREPARED:
                        self.MsgBuffer.PrepareMsgs.append(msg)
                    else:
                        self.MsgBuffer.PrepareMsgs.append(msg)
                        self.MsgDelivery.extend(self.MsgBuffer.PrepareMsgs)
                        self.MsgBuffer.PrepareMsgs = []
                elif msg.MsgType == MsgType.COMMITMSG:
                    if (self.CurrentState is None) or self.CurrentState.CurrentStage != stage.PREPARED:
                        self.MsgBuffer.CommitMsgs.append(msg)
                    else:
                        self.MsgBuffer.CommitMsgs.append(msg)
                        self.MsgDelivery.extend(self.MsgBuffer.CommitMsgs)
                        self.MsgBuffer.CommitMsgs = []
                elif msg.MsgType == MsgType.INVOLVEPREPAREMSG:
                    # print("msg.MsgType == MsgType.INVOLVEPREPAREMSG",flush=True)
                    if (self.CurrentState is None) or self.CurrentState.CurrentStage != stage.INVOLVINGPREPREPARED:
                        # print("msg.MsgType == MsgType.INVOLVEPREPAREMSG if {} {}".format(self.CurrentState.CurrentStage,len(self.MsgBuffer.PrepareMsgs)),flush=True)
                        self.MsgBuffer.PrepareMsgs.append(msg)
                    else:
                        # print("msg.MsgType == MsgType.INVOLVEPREPAREMSG else {} {}".format(self.CurrentState.CurrentStage,len(self.MsgBuffer.PrepareMsgs)),flush=True)
                        self.MsgBuffer.PrepareMsgs.append(msg)
                        self.MsgDelivery.extend(self.MsgBuffer.PrepareMsgs)
                        self.MsgBuffer.PrepareMsgs = []
                elif msg.MsgType == MsgType.INVOLVECOMMITMSG:
                    if (self.CurrentState is None) or self.CurrentState.CurrentStage != stage.INVOLVINGPREPARED:
                        self.MsgBuffer.CommitMsgs.append(msg)
                        # print("msg.MsgType == MsgType.INVOLVECOMMITMSG if {} {}".format(self.CurrentState.CurrentStage,len(self.MsgBuffer.PrepareMsgs)),flush=True)
                    else:
                        self.MsgBuffer.CommitMsgs.append(msg)
                        self.MsgDelivery.extend(self.MsgBuffer.CommitMsgs)
                        self.MsgBuffer.CommitMsgs = []
                        # print(
                        #     "msg.MsgType == MsgType.INVOLVECOMMITMSG else", flush=True)
            elif msgType == ReplyMsg:
                # print("Result: {} by {} on {}".format(
                #     msg.Result, msg.NodeID, msg.Timestamp), flush=True)
                if (self.CurrentState is None) or self.CurrentState.CurrentStage == stage.COMMITED or self.CurrentState.CurrentStage == stage.INVOLVINGCOMMITED:
                    # print("if {}".format(len(self.MsgBuffer.ReplyMsgs)), flush=True)
                    self.MsgBuffer.ReplyMsgs.append(msg)
                    self.MsgDelivery.extend(self.MsgBuffer.ReplyMsgs)
                    self.MsgBuffer.ReplyMsgs = []
                else:
                    # print("else {}".format(
                    #     len(self.MsgBuffer.ReplyMsgs)), flush=True)
                    self.MsgBuffer.ReplyMsgs.append(msg)
                # for i in self.MsgDelivery:
                #     print("After {}".format(i),flush=True)
            elif msgType == GlobalForwardMsg:
                if self.CurrentState is None or self.CurrentState.CurrentStage == stage.COMMITED:
                    self.MsgDelivery.append(msg)
            elif msgType == LocalForwardMsg:
                if self.CurrentState is None:
                    self.MsgBuffer.LocalForwardMsgs.append(msg)
                else:
                    self.MsgBuffer.LocalForwardMsgs.append(msg)
                    self.MsgDelivery.extend(self.MsgBuffer.LocalForwardMsgs)
                    self.MsgBuffer.LocalForwardMsgs = []

    # 片内广播
    def Broadcast(self, msg, route):
        # print(type(msg),flush=True)
        if msg is not None:
            for key in self.NodeTable.keys():
                if key != self.NodeID and key[1] == self.NodeID[1]:
                    requests.post(
                        "http://" + self.NodeTable[key] + route, json=json.dumps(msg))
                    print('[Send] ' + self.NodeTable[key] + route, flush=True)

    # 片间通信
    def GlobalShare(self, globalForwardMsg: GlobalForwardMsg, sourceStage):
        if sourceStage == "InvolveForward" and self.CurrentState is not None:
            try:
                print("{} {}".format(type(globalForwardMsg.ReqMsg),
                                     type(globalForwardMsg.CommitMsg)), flush=True)
                requests.post("http://" + self.NodeTable[self.NodeID[0] + str(
                    int(self.NodeID[1]) % 3 + 1) + self.NodeID[-2:]] + '/globalforward', json=json.dumps(EncodeGlobalForwardMsg(globalForwardMsg)))
                print('[Send] ' + self.NodeTable[self.NodeID[0] + str(
                    int(self.NodeID[1]) % 3 + 1) + self.NodeID[-2:]] + '/globalforward', flush=True)
            except Exception as e:
                print('[Send Failed] {} as {}'.format(
                    self.NodeID[0] + str(int(self.NodeID[1]) % 3 + 1) + self.NodeID[-2:], e), flush=True)
            # pass
        else:
            tRingOrder = globalForwardMsg.ReqMsg.RingOrder[0]
            nodeIndex = tRingOrder.index(int(self.NodeID[1]))
            tRingFrequency = globalForwardMsg.ReqMsg.RingOrder[1]
            if sourceStage == "GetCommit" and self.CurrentState is not None:
                msg = EncodeGlobalForwardMsg(globalForwardMsg)
                if tRingFrequency[nodeIndex] == 1:
                    self.CurrentState.CurrentStage = stage.EXECUTED
                if tRingFrequency.count(2) == 0:
                    if nodeIndex == len(tRingOrder) - 1:
                        print("This Transaction Executed Completely. \n{} : {}".format(TimestampToDate(
                            self.CurrentState.MsgLogs.ReqMsg.Timestamp), self.CurrentState.MsgLogs.ReqMsg.Operation), flush=True)
                        self.RestartNode()
                    else:
                        print('[Send] ' + self.NodeTable[self.NodeID[0] + str(tRingOrder[(nodeIndex+1) % len(
                            tRingOrder)]) + self.NodeID[-2:]] + '/globalforward', flush=True)
                        requests.post("http://" + self.NodeTable[self.NodeID[0] + str(tRingOrder[(nodeIndex+1) % len(
                            tRingOrder)]) + self.NodeID[-2:]] + '/globalforward', json=json.dumps(msg))
                else:
                    if nodeIndex != len(tRingOrder) - 1:
                        print('[Send] ' + self.NodeTable[self.NodeID[0] + str(tRingOrder[(nodeIndex+1) % len(
                            tRingOrder)]) + self.NodeID[-2:]] + '/globalforward', flush=True)
                        requests.post("http://" + self.NodeTable[self.NodeID[0] + str(tRingOrder[(nodeIndex+1) % len(
                            tRingOrder)]) + self.NodeID[-2:]] + '/globalforward', json=json.dumps(msg))
                    else:
                        print('[Send] ' + self.NodeTable[self.NodeID[0] + str(tRingOrder[nextShard(
                            nodeIndex, tRingFrequency, "GetCommit")]) + self.NodeID[-2:]] + '/globalforward', flush=True)
                        requests.post("http://" + self.NodeTable[self.NodeID[0] + str(tRingOrder[nextShard(
                            nodeIndex, tRingFrequency, "GetCommit")]) + self.NodeID[-2:]] + '/globalforward', json=json.dumps(msg))
            elif sourceStage == "GetLocalForward" and self.CurrentState is not None:
                nextShardIndex = nextShard(
                    nodeIndex, tRingFrequency, "GetLocalForward")

                # print("sourceStage == GetLocalForward {} {} {} {}".format(nextShardIndex,tRingOrder,nodeIndex,tRingFrequency),flush=True)

                if nextShardIndex < nodeIndex:
                    print("This Transaction Executed Completely. {} : {}".format(TimestampToDate(
                        self.CurrentState.MsgLogs.ReqMsg.Timestamp), self.CurrentState.MsgLogs.ReqMsg.Operation), flush=True)
                else:
                    msg = EncodeGlobalForwardMsg(globalForwardMsg)
                    print('[Send] ' + self.NodeTable[self.NodeID[0] + str(
                        tRingOrder[nextShardIndex]) + self.NodeID[-2:]] + '/globalforward', flush=True)
                    requests.post("http://" + self.NodeTable[self.NodeID[0] + str(
                        tRingOrder[nextShardIndex]) + self.NodeID[-2:]] + '/globalforward', json=json.dumps(msg))

    def RestartNode(self):
        if self.CurrentState is not None:
            self.LastConsensusMsgs = self.CurrentState.MsgLogs
            self.CurrentState = None
            self.committedMsgs = []
            self.MsgBuffer = MsgBuffer([], [], [], [], [], [], [])
            self.MsgEntrance = []
            for msg in self.MsgDelivery:
                if type(msg) != ReplyMsg:
                    self.MsgDelivery.remove(msg)

    def getExecute(self, operation: str, result: bool):
        operations = operation.split(" ")
        # print("getExecute {}".format(self.CurrentState.CurrentStage),flush=True)
        if operations[0] == "InvolveRequest":
            if result == False:
                self.NodeTable.pop(operations[1])
            else:
                if operations[1] not in self.NodeTable:
                    self.NodeTable[operations[1]
                                   ] = "127.0.0.1:" + operations[2]
                if self.CurrentState is not None and type(self.CurrentState.MsgLogs.ReqMsg) == RequestMsg:
                    msg = GlobalForwardMsg(self.CurrentState.MsgLogs.ReqMsg, self.CurrentState.MsgLogs.CommitMsgs[self.NodeID], getDigest(
                        self.CurrentState.MsgLogs.CommitMsgs[self.NodeID]))
                    self.GlobalShare(msg, "InvolveForward")

        for node in self.NodeTable.keys():
            print("NodeTable : {} {}".format(
                node, self.NodeTable[node]), flush=True)


def nextShard(nodeIndex, tRingFrequency: list, sStage):
    if sStage == "GetCommit":
        if tRingFrequency[:nodeIndex].count(2) != 0:
            return tRingFrequency.index(2, 0, nodeIndex)
        else:
            return -1
    elif sStage == "GetLocalForward":
        if tRingFrequency[nodeIndex+1:].count(2) != 0:
            return tRingFrequency.index(2, nodeIndex+1)
        else:
            return -1


class Server:
    def __init__(self, myPort: int, myNode: Node) -> None:
        self.Port = myPort
        self.Node = myNode


def LogMsg(msg):
    msgType = type(msg)
    if msgType == RequestMsg:
        print("[REQUEST] ClientID: {}, Timestamp: {}, Operation: {}".format(
            msg.ClientID, msg.Timestamp, msg.Operation), flush=True)
    elif msgType == PrePrepareMsg:
        print("[PREPREPARE] ClientID: {}, Operation: {}, SequenceID: {}".format(
            msg.RequestMsg.ClientID, msg.RequestMsg.Operation, msg.SequenceID), flush=True)
    elif msgType == VoteMsg:
        if msg.MsgType == MsgType.PREPAREMSG:
            print("[PREPARE] NodeID: {}".format(msg.NodeID), flush=True)
        elif msg.MsgType == MsgType.COMMITMSG:
            print("[COMMIT] NodeID: {}".format(msg.NodeID), flush=True)
    elif msgType == GlobalForwardMsg:
        print("[Global Sharing] ReqMsg: {}".format(msg.ReqMsg), flush=True)


def NewNode(nodeID, nodeTable: dict, view: View, currentState, committedMsgs: list, msgBuffer, msgEntrance, msgDelivery):
    node = Node(nodeID, nodeTable, view, currentState, committedMsgs,
                msgBuffer, msgEntrance, msgDelivery, MsgLogs(None, [], {}, {}, {}))
    threading.Thread(target=node.DispatchMsg).start()
    threading.Thread(target=node.ResolveMsg).start()
    return node


if __name__ == "__main__":
    print(nextShard(2, [2, 2, 1], "GetLocalForward"))
