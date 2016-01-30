import json
from webob import Response

from ryu.base import app_manager
from ryu.controller import ofp_event, dpset
from ryu.controller.handler import set_ev_cls, CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.ofproto import ofproto_v1_3
from ryu.lib import dpid as dpid_lib
from ryu.lib.packet import packet, ethernet, ether_types
from ryu.app.wsgi import ControllerBase, WSGIApplication

from kazoo.client import KazooClient

from sys import argv
    
SWITCHID_PATTERN = dpid_lib.DPID_PATTERN + r'|all'

class TestRoleController(app_manager.RyuApp):
    def __init__(self, *args, **kwargs):
        #controller init
        super(TestRoleController, self).__init__(*args, **kwargs)
        self.zk = KazooClient()
        self.zk.start()

        self.dp = {}
        self.mac_to_port = {}
        self.generation_id = 0

    def close(self):
        self.zk.stop()
        self.logger.info("zookeeper stop")

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_feartures_handler(self, ev):
        msg = ev.msg

        self.logger.debug('OFPSwitchFeatures received: \n'
                          'datapath_id=0x%016x n_buffers=%d \n'
                          'n_tables=%d auxiliary_id=%d \n'
                          'capabilities=0x%08x',
                          msg.datapath_id, msg.n_buffers, msg.n_tables,
                          msg.auxiliary_id, msg.capabilities)

        dp = msg.datapath 
        ofproto = dp.ofproto 
        parser = dp.ofproto_parser

        match = parser.OFPMatch() #OFPMatch return dict
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(dp, 0, match, actions)
        
    def add_flow(self, dp, priority, match, actions, buffer_id=None):

        ofproto = dp.ofproto
        parser = dp.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                         actions)]

        self.logger.debug('add_flow: '
                          'match: %s \n'
                          'actions: %s \n'
                          'inst: %s',
                          match, actions, inst)

        if buffer_id:
            mod = parser.OFPFlowMod(datapath=dp, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst)
        else:
            mod = parser.OFPFlowMod(datapath=dp, priority=priority,
                                   match=match, instructions=inst)

        dp.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):

        msg = ev.msg
        if msg.msg_len < msg.total_len:
            self.logger.debug("packet truncated: only %s of %s bytes",
                              msg.msg_len, msg.total_len)
        
        self.logger.debug('OFPPacketIn received: '
                          'buffer_id=%x total_len=%d reason=%s \n'
                          'table_id=%d cookie=%d match=%s',
                          msg.buffer_id, msg.total_len, msg.reason,
                          msg.table_id,msg.cookie, msg.match)
                          

        dp = msg.datapath
        ofproto = dp.ofproto
        parser = dp.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            #ignore lldp packet ??what??
            return

        dst = eth.dst
        src = eth.src

        dpid = dp.id
        self.mac_to_port.setdefault(dpid, {})

        self.logger.info("packet in %s %s %s %s", dpid, src, dst, in_port)
        
        #learn a mac address
        self.mac_to_port[dpid][src] = in_port

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD  #0001

        actions = [parser.OFPActionOutput(out_port)]

        data = None

        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=dp, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        dp.send_msg(out)

    def send_role_request(self, dp, role="nochange"):
        self.logger.info('send role request %s', role)

        ofp = dp.ofproto
        ofp_parser = dp.ofproto_parser
        if role == "nochange":
            req = ofp_parser.OFPRoleRequest(dp, ofp.OFPCR_ROLE_NOCHANGE, self.generation_id)
        elif role == "equal":
            req = ofp_parser.OFPRoleRequest(dp, ofp.OFPCR_ROLE_EQUAL, self.generation_id)
        elif role == "master":
            req = ofp_parser.OFPRoleRequest(dp, ofp.OFPCR_ROLE_MASTER, self.generation_id)
            self.generation_id += 1 
        elif role == "slave":
            req = ofp_parser.OFPRoleRequest(dp, ofp.OFPCR_ROLE_SLAVE, self.generation_id)
        else:
            self.logger.error('undefined role %s',role)

        dp.send_msg(req)
    
    @set_ev_cls(ofp_event.EventOFPRoleReply, MAIN_DISPATCHER)
    def role_reply_handler(self, ev):
        msg = ev.msg
        dp = msg.datapath
        ofp = dp.ofproto

        if msg.role == ofp.OFPCR_ROLE_NOCHANGE:
            role = 'NOCHANGE'
        elif msg.role == ofp.OFPCR_ROLE_EQUAL:
            role = 'EQUAL'
        elif msg.role == ofp.OFPCR_ROLE_MASTER:
            role = 'MASTER'
        elif msg.role == ofp.OFPCR_ROLE_SLAVE:
            role = 'SLAVE'
        else:
            role = 'unknown'

        self.logger.debug('OFPRoleReply received: '
                          'role=%s generation_id=%d',
                          role, msg.generation_id)
    
    @set_ev_cls(dpset.EventDP, dpset.DPSET_EV_DISPATCHER)
    def datapath_handler(self, ev):
        dpid = dpid_lib.dpid_to_str(ev.dp.id)
        if ev.enter:
            self.dp[dpid] = ev.dp
        else:
            del self.dp[dpid]
        

class RestController(ControllerBase):
    def __init__(self, req, link, data, **config):
        super(RestController, self).__init__(req, link, data, **config)
        #self.controller = data['TestRoleController'] 
        self.controller = data

    def hello_ryu(self, req, **kwargs):
        msg = json.dumps("Hello, Ryu!!")
        return Response(content_type = "application/json", body = msg)
    
    def send_role_master(self, req, switch_id, **kwargs):
        msg = json.dumps("send request master")
        dp = self.controller.dp[switch_id]
        self.controller.send_role_request(dp, role="master")
        return Response(content_type = 'application/json', body = msg)

    def send_role_slave(self, req, switch_id, **kwargs):
        msg = json.dumps("send request slave")
        dp = self.controller.dp[switch_id]
        self.controller.send_role_request(dp, role="slave")
        return Response(content_type = 'application/json', body = msg)
    
    def send_role_equal(self, req, switch_id, **kwargs):
        msg = json.dumps("send request equal")
        dp = self.controller.dp[switch_id] 
        self.controller.send_role_request(dp, role="equal")
        return Response(content_type = 'application/json', body = msg)
        

class RestAPI(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {'wsgi': WSGIApplication,
                 'controller':TestRoleController}

    def __init__(self, *args, **kwargs):
        super(RestAPI, self).__init__(*args, **kwargs)

        self.mac_to_port = {}
        self.requirements = {'switch_id': SWITCHID_PATTERN}
        self.start_rest(**kwargs)

    def start_rest(self, **kwargs):
        """
        seting rest api
        """
        self.controller = kwargs['controller']
        wsgi = kwargs['wsgi']
        wsgi.registory['RestController'] = self.controller
        mapper = wsgi.mapper
        mapper.connect('hello', '/hello_ryu', 
                       controller = RestController,
                       action = 'hello_ryu',
                       conditions = dict(method = ['GET']))
        mapper.connect('rest', '/role/master/{switch_id}',
                       controller = RestController,
                       requirements = self.requirements,
                       action = 'send_role_master',
                       conditions = dict(method = ['GET']))
        mapper.connect('rest', '/role/slave/{switch_id}',
                       controller = RestController,
                       requirements = self.requirements,
                       action = 'send_role_slave',
                       conditions = dict(method = ['GET']))
        mapper.connect('rest', '/role/equal/{switch_id}',
                       controller = RestController,
                       requirements = self.requirements,
                       action = 'send_role_equal',
                       conditions = dict(method = ['GET']))
