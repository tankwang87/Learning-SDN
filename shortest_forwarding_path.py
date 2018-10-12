from ryu.base import app_manager
from ryu.ofproto import ofproto_v1_3
from ryu.controller.handler import set_ev_cls
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller import ofp_event
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.topology import event
from ryu.topology.api import get_switch, get_link
import networkx as nx


#inherit from simple_switch_13 APP to get L2 switch capacity
class ShortestForwardingPath(app_manager.RyuApp):
    """a app detemine shortest forwarding path"""
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(ShortestForwardingPath, self).__init__(*args, **kwargs)
        self.network = nx.DiGraph()
        self.topology_api_app = self
        #dict to store shortest path
        self.paths = {}



    #handle switch feathures info
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_feature_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        #install a table-miss flow entry fo each datapath
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        #install flow entry
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        #contruct a flow_mod msg and send it to datapath
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                match=match, instructions=inst)
        datapath.send_msg(mod)

    #get topology and store it into networkx object
    @set_ev_cls(event.EventSwitchEnter, [CONFIG_DISPATCHER, MAIN_DISPATCHER])
    def get_topology(self, ev):
        #get nodes
        switch_list = get_switch(self.topology_api_app, None)
        switches = [switch.dp.id for switch in switch_list]
        #add switch list into networkx object at one time
        self.network.add_nodes_from(switches)

        #get links
        links_list = get_link(self.topology_api_app, None)
        links = [(link.src.dpid, link.dst.dpid, {'port': link.src.port_no}) for link in links_list]
        self.network.add_edges_from(links)

        #reverse links
        links = [(link.dst.dpid, link.src.dpid, {'port':link.dst.port_no}) for link in links_list]
        self.network.add_edges_from(links)

    #get out_port by using networkx's Dijkstra algorithm
    def get_out_port(self, datapath, src, dst, in_port):
        dpid = datapath.id
        #add links between hosts and access switches
        if src not in self.network:
            self.network.add_node(src)
            #link from switch to host
            #work with networkx v1.1
            #self.network.add_edge(dpid, src, {'port': in_port})
            #work with network v2.2
            self.network.add_edge(dpid, src, port = in_port)
            self.network.add_edge(src, dpid)
            self.paths.setdefault(src, {})

        #search   dst,s shortest path
        if dst in self.network:
            if dst not in self.paths[src]:
                path = nx.shortest_path(self.network, src, dst)
                self.paths[src][dst] = path
            path = self.paths[src][dst]
            print "path: ", path
            #now we know the forwarding chain between src switch to dst switch to transmit pkt from src to dst
            #we need to know which port connet src switch with next hop switch to forward pkt
            next_hop = path[path.index(dpid) + 1]
            out_port = self.network[dpid][next_hop]['port']

        else:
            out_port = datapath.ofproto.OFPP_FLOOD

        return out_port

    #handle packet msg
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        in_port = msg.match['in_port']

        #get out_port
        out_port = self.get_out_port(datapath, eth.src, eth.dst, in_port)
        actions = [parser.OFPActionOutput(out_port)]

        #install flow entries
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port = in_port, eth_dst = eth.dst)
            self.add_flow(datapath, 1, match, actions)

        #send packet_out msg to datapath
        out = parser.OFPPacketOut(
              datapath = datapath, buffer_id = msg.buffer_id, in_port = in_port,
              actions = actions)
        datapath.send_msg(out)
