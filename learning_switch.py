from ryu.base import app_manager
from ryu.ofproto import ofproto_v1_3
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.controller import ofp_event
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet

class LearningSwitch(app_manager.RyuApp):
    """a switch learn mac from packet and construct flow entry"""
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(LearningSwitch, self).__init__(*args, **kwargs)
        self.mac_to_port = {}

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_feature_handler(self, ev):
        '''config switch when it connects'''
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        #install table-miss flow entry
        match = parser.OFPMatch()
        #set switch : send unknown packet to contoller && do not store unknown packet in buffer
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                         ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions, buffer_id = None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        #construct a flow mod msg
        #set instruction : apply all actions on the switch immediately
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                            actions)]
        mod = parser.OFPFlowMod(datapath = datapath, priority = priority,
                                match = match, instructions = inst)

        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        #get datpath id to identify openflow switch
        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})

        #store the info
        #parser and analyze the received packet
        pkt = packet.Packet(msg.data)
        eth_pkt = pkt.get_protocol(ethernet.ethernet)
        dst = eth_pkt.dst
        src= eth_pkt.src
        in_port = msg.match['in_port']
        self.logger.info("packet in %s %s %s %s", dpid, src, dst, in_port)

        #learn a src mac to port mapping to avoid flood next time
        self.mac_to_port[dpid][src] = in_port
        #if the dst mac has learned ,decide output port. otherwise flood the packet
        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        #construct actions
        actions = [parser.OFPActionOutput(out_port)]

        # install a flow mod msg(if dst mapping not found, flood packet directly and do not install flow)
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port = in_port, eth_dst = dst)
            self.add_flow(datapath, 1, match, actions)

        #send packet out
        out = parser.OFPPacketOut(
              datapath = datapath, buffer_id = msg.buffer_id, in_port = in_port,
              actions = actions)

        datapath.send_msg(out)
