#!/usr/bin/env python3
"""Routed topology for the RTP project (run with: sudo python3 topology/topo.py)

   h1 --[10.0.1.0/24]-- r1 --[10.0.2.0/24]-- r2 --[10.0.3.0/24]-- h2

   h1: 10.0.1.10   r1: 10.0.1.1 / 10.0.2.1   r2: 10.0.2.2 / 10.0.3.1   h2: 10.0.3.10
   Routing: static. Impairments (netem/tbf) are added later on the r1-r2 link.
"""

from mininet.cli import CLI
from mininet.log import setLogLevel
from mininet.net import Mininet
from mininet.node import Node


class LinuxRouter(Node):
    """A host with IP forwarding enabled."""

    def config(self, **params):
        super().config(**params)
        self.cmd("sysctl -w net.ipv4.ip_forward=1")

    def terminate(self):
        self.cmd("sysctl -w net.ipv4.ip_forward=0")
        super().terminate()


def build():
    net = Mininet(controller=None, autoSetMacs=True)

    h1 = net.addHost("h1", ip=None)
    h2 = net.addHost("h2", ip=None)
    r1 = net.addHost("r1", cls=LinuxRouter, ip=None)
    r2 = net.addHost("r2", cls=LinuxRouter, ip=None)

    # subnet A: h1 - r1
    net.addLink(h1, r1, intfName1="h1-eth0", intfName2="r1-eth0",
                params1={"ip": "10.0.1.10/24"}, params2={"ip": "10.0.1.1/24"})
    # subnet B: r1 - r2 (impairments go here)
    net.addLink(r1, r2, intfName1="r1-eth1", intfName2="r2-eth0",
                params1={"ip": "10.0.2.1/24"}, params2={"ip": "10.0.2.2/24"})
    # subnet C: r2 - h2
    net.addLink(r2, h2, intfName1="r2-eth1", intfName2="h2-eth0",
                params1={"ip": "10.0.3.1/24"}, params2={"ip": "10.0.3.10/24"})

    net.start()

    # static routes
    h1.cmd("ip route add default via 10.0.1.1")
    h2.cmd("ip route add default via 10.0.3.1")
    r1.cmd("ip route add 10.0.3.0/24 via 10.0.2.2")
    r2.cmd("ip route add 10.0.1.0/24 via 10.0.2.1")

    return net


if __name__ == "__main__":
    setLogLevel("info")
    net = build()
    CLI(net)
    net.stop()
