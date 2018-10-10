from operator import attrgetter
from ryu.app import simple_switch_13
from ryu.ofproto import ofproto_v1_3
from ryu.controller.handler import set_ev_cls
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.lib import hub


#inherit from simple_switch_13 APP to get L2 switch capacity
class Monitor13(simple_switch_13.SimpleSwitch13):
    """a app monitor switch port traffic"""
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(Monitor13, self).__init__(*args, **kwargs)
        self.datapaths = {}
        self.monitor_thread = hub.spawn(self._monitor)      #new a thread to implement monitor founction, whie main thread run others

    #get datapath info
    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:         #judge if the switch(datapath) has been recorded in APP
                self.datapaths[datapath.id] = datapath
                self.logger.debug("Register datapath %16x", datapath.id)

        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                del self.datapaths[datapath.id]
                self.logger.debug("Unregister datapath %16x", datapath.id)

    #send request msg periodically
    def _monitor(self):
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(5)       #sleep 5s

    #send port&flow stats request msg to datapath
    def _request_stats(self, datapath):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # send port stats req msg
        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
        datapath.send_msg(req)

        #send flow stats req msg
        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)
        self.logger.debug("send stats req to datapath: %16x", datapath.id)

    #handle port stats rereply msg
    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stat_repaly_hendler(self, ev):
        body = ev.msg.body

        self.logger.info("**************Port MSG of %x**************", ev.msg.datapath.id)
        self.logger.info('datapath                port           '
                         'rx-pkt        rx-byte     rx-errors    '                    #port received info
                         'tx-pkt        tx-byte     tx-errors')                   #port transmittd info
        self.logger.info('----------------------  ------------  '
                         '----------  ------------  ----------   '
                         '----------  ------------  ---------- ')
        for stat in sorted(body, key = attrgetter('port_no')):
            self.logger.info("%16x    %8x          %8d  %8d  %8d          %8d          %8d  %8d  ",
                             ev.msg.datapath.id, stat.port_no,
                             stat.rx_packets, stat.rx_bytes, stat.rx_errors,
                             stat.tx_packets, stat.tx_bytes, stat.tx_errors)
        self.logger.info("**************End of Port MSG************** \n")


    # handle flow stats rereply msg
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stat_handler(self, ev):
        body = ev.msg.body

        self.logger.info("**************Flow MSG of %x**************", ev.msg.datapath.id)
        self.logger.info('datapath                       '
                         'in_port        eth-dst               '
                         'out_port      packets        bytes')
        self.logger.info('----------------------        '
                         '----------  -----------------------  '
                         '----------  ------------  ----------')
        for stat in sorted([flow for flow in body if flow.priority == 1],                    # filter table miss flow entry, and sort rest flow
                           key = lambda flow : (flow.match['in_port'],
                            flow.match['eth_dst'])):
            self.logger.info("%16x          %8x         %s      %8d      %8d      %8d",
                             ev.msg.datapath.id,
                             stat.match['in_port'], stat.match['eth_dst'],
                             stat.instructions[0].actions[0].port,
                             stat.packet_count, stat.byte_count,)
        self.logger.info("**************End of Port MSG************** \n")
