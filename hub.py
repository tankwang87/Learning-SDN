from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3


class Hub(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(Hub, self).__init__(*args, **kwargs)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_feature_handler(self, ev):
        '''config switch when it connects'''
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        match = parser.OFPMatch()
        #set switch : send unknown packet to contoller && do not store unknown packet in buffer
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                         ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        #construct a flow entry
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_FLOOD)]

        #install flow to avoid same packet next time
        self.add_flow(datapath, 1, match, actions)
        out = parser.OFPPacketOut(
            datapath = datapath, buffer_id = msg.buffer_id, in_port = in_port,
            actions =actions)
        datapath.send_msg(out)

    def add_flow(self, datapath, priority, match, actions, buffer_id = None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        #set instruction : apply all actions on the switch immediately
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                            actions)]
        mod = parser.OFPFlowMod(datapath = datapath, priority = priority,
                                match = match, instructions = inst)

        datapath.send_msg(mod)