# Test DB queries in Neutron.

import random

import netaddr
from netaddr.strategy import eui48
from neutron_lib.db import api as db_api
from neutron_lib.db import standard_attr
from neutron_lib import context
from oslo_db import options
from oslo_config import cfg
import sqlalchemy
from sqlalchemy import and_
from sqlalchemy import exists
import testtools

from neutron.db import models_v2
from neutron.db.models import network_segment_range
from neutron.db.models.plugins.ml2 import geneveallocation
from neutron.db.models.plugins.ml2 import vlanallocation
from neutron.db.qos import models as qos_db_model
from neutron.objects import network as network_obj
from neutron.objects import ports as port_obj
from neutron.objects.qos import policy as qos_policy_obj
from neutron.tests.unit import testlib_api


# Choose one!
#DB_SELECTOR = 'in_memory'
DB_SELECTOR = 'local'

CONNECTION_URL = 'mysql+pymysql://root:password@127.0.0.1/neutron?charset=utf8'
options.set_defaults(cfg.CONF, connection=CONNECTION_URL)
DB_OPTS = [cfg.StrOpt('connection', default=CONNECTION_URL)]


class InMemoryDB(testlib_api.BaseSqlTestCase, testtools.TestCase):

    def __init__(self):
        self.runTest = None
        super(InMemoryDB, self).__init__()
        self.setUp()
        self.context = context.get_admin_context()

    def list_tables(self):
        inspector = sqlalchemy.inspect(self.engine)
        return inspector.get_table_names()

    def query_table(self, db_model):
        return self.context.session.query(db_model).all()


class LocalDB(object):

    def __init__(self):
        self.context = context.get_admin_context()
        self._connection_url = CONNECTION_URL

    def list_tables(self):
        self._engine = sqlalchemy.create_engine(self._connection_url)
        self._connection = self._engine.connect()
        inspector = sqlalchemy.inspect(self._engine)
        return inspector.get_table_names()

    def query_table(self, db_model):
        return self.context.session.query(db_model).all()


def create_port(test_db, network_id, qos_policy=None):
    mac = 0xcafecafe0000 + random.randint(0, 1000)
    mac_address = netaddr.EUI(mac, dialect=eui48.mac_unix_expanded)
    qos_policy_id = qos_policy.id if qos_policy else None
    return port_obj.Port(test_db.context, name='port_qos1',
                         network_id=network_id, qos_policy_id=qos_policy_id,
                         mac_address=mac_address, admin_state_up=False,
                         device_id='1', device_owner='my_port', status='DOWN')


if DB_SELECTOR == 'in_memory':
    test_db = InMemoryDB()
elif DB_SELECTOR == 'local':
    test_db = LocalDB()
else:
    raise Exception('Please, choose between "in_memory" or "local"')


# Example 1.
# Read tables created
tables = test_db.list_tables()


# Example 2.
# Create new network using the OVO interface. That will create several DB
# registers.
with db_api.CONTEXT_WRITER.using(test_db.context):
    networks_before = test_db.query_table(models_v2.Network)

    network = network_obj.Network(test_db.context, name='net1')
    network.create()
    networks_after = test_db.query_table(models_v2.Network)
    assert len(networks_after) == len(networks_before) + 1

    attrs = test_db.query_table(standard_attr.StandardAttribute)

    network.delete()
    networks_after_delete = test_db.query_table(models_v2.Network)
    assert len(networks_after_delete) == len(networks_before)


# Example 3.
# https://review.opendev.org/#/c/714617/2/neutron/objects/ports.py
# Retrieve all ports with and without IP address allocation from a network.
with db_api.CONTEXT_WRITER.using(test_db.context):
    # We assume "private" network exists in the DB; if devstack is used, it
    # will be created by default.
    # https://docs.sqlalchemy.org/en/13/orm/query.html#sqlalchemy.orm.query.Query.filter
    network = test_db.context.session.query(
        models_v2.Network).filter_by(name='private').first()
    # https://docs.sqlalchemy.org/en/13/orm/query.html#sqlalchemy.orm.query.Query.filter_by
    network_idem = test_db.context.session.query(
        models_v2.Network).filter(models_v2.Network.name == 'private').first()

    subnet_id = network.subnets[0].id
    port1 = create_port(test_db, network.id)
    port1.create()
    port2 = create_port(test_db, network.id)
    port2.create()
    ipalloc = port_obj.IPAllocation(test_db.context, port_id=port2.id,
                                    subnet_id=subnet_id, network_id=network.id,
                                    ip_address=netaddr.IPAddress('10.2.0.1'))
    ipalloc.create()

    ports_with_ip_address = test_db.context.session.query(
        models_v2.Port).filter(and_(
        models_v2.IPAllocation.port_id == models_v2.Port.id,
        models_v2.IPAllocation.network_id == network.id)).all()
    ports_with_ip_address_ids = [p.id for p in ports_with_ip_address]
    assert port2.id in ports_with_ip_address_ids

    ports_without_ip_address = test_db.context.session.query(
        models_v2.Port).filter(~exists().where(
        models_v2.IPAllocation.port_id == models_v2.Port.id)).filter(
        models_v2.Port.network_id == network.id).all()
    assert len(ports_without_ip_address) == 1
    assert port1.id == ports_without_ip_address[0].id

    port1.delete()
    port2.delete()


# Example 4.
# https://review.opendev.org/#/c/712508/6/neutron/objects/network_segment_range.py
# Retrieve, from the default segment range, the allocated segment IDs. This
# example retrieves two types: "geneve" and "vlan" (we need to add an extra
# filter using the physical network).
with db_api.CONTEXT_READER.using(test_db.context):
    srange_model = network_segment_range.NetworkSegmentRange
    geneve_model = geneveallocation.GeneveAllocation
    query = test_db.context.session.query(geneve_model)
    query = query.filter_by(allocated=True)
    query = query.join(srange_model, srange_model.network_type == 'geneve')
    query = query.filter(and_(
        geneve_model.geneve_vni >= srange_model.minimum,
        geneve_model.geneve_vni <= srange_model.maximum,
        srange_model.project_id == None))
    allocated_geneve_segments_from_default_range = query.all()

    vlan_model = vlanallocation.VlanAllocation
    query = test_db.context.session.query(vlan_model)
    query = query.filter_by(allocated=True)
    query = query.join(
        srange_model,
        and_(srange_model.network_type == 'vlan',
             srange_model.physical_network == vlan_model.physical_network))
    query = query.filter(and_(
        vlan_model.vlan_id >= srange_model.minimum,
        vlan_model.vlan_id <= srange_model.maximum,
        srange_model.project_id == None))
    allocated_vlan_segments_from_default_range = query.all()


# Example 5.
# https://review.opendev.org/#/c/711317/17/neutron/objects/qos/binding.py
# Retrieve, from a specific network, those ports with and without QoS policy
# bound.
with db_api.CONTEXT_WRITER.using(test_db.context):
    network_id = test_db.context.session.query(
        models_v2.Network.id).filter_by(name='private').first()[0]

    qos_policy = qos_policy_obj.QosPolicy(test_db.context, name='qos1')
    qos_policy.create()
    port = create_port(test_db, network_id, qos_policy=qos_policy)
    port.create()

    query = test_db.context.session.query(models_v2.Port).filter(
        models_v2.Port.network_id == network_id)
    port_in_network_with_qos_policy = query.filter(exists().where(and_(
        qos_db_model.QosPortPolicyBinding.port_id == models_v2.Port.id,
        qos_db_model.QosPortPolicyBinding.policy_id == qos_policy.id))).all()
    assert len(port_in_network_with_qos_policy) == 1
    assert port_in_network_with_qos_policy[0].id == port.id

    port_in_network_without_qos_policy = query.filter(~exists().where(
        qos_db_model.QosPortPolicyBinding.port_id ==
        models_v2.Port.id)).filter(models_v2.Port.network_id ==
                                   network_id).all()

    port.delete()
    qos_policy.delete()
