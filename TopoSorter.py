#!/usr/bin/env python3

'''
 TopoSorter -- Simple program to sort nodes from topology.json file by their
 RTT (specific to Cardano project). The intended audience vary from SPO to
 Daedalus users (may help in making sensible choices for topology.yaml)

  This file is part of TopoSorter.

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see <https://www.gnu.org/licenses/>.

 Copyright (c) 2021  Raphaël Bazaud <rbazaud@pm.me>
'''

import json
import pandas as pd
import requests as rq
import socket as sock
import time as tm

'''
TopoSorter is written in Python 3.x and depends on python3-pandas for DataFrame

Please note there are (a few) limitations :

    * as I have been gently reminded by my preferred SPO, given the P2P upgrade
    in progress on Cardano this tool will not be useful for long.
    * we rely on a connect() call to approximate RTT, it is far to be perfect
    but the objective here is to sort, not to get a usec order precision.
    * connect() implementation is relying at some point on getaddrinfo() and
    using empirical approach it takes a long time in some case (esp. ddns)
    * the current version does not allow for filtering on a specific continent
    (predicate matching could be implemented with filter() though) We believe
    it might serves the end-user better by making no (geographic) assumption.
    That also avoids the user having to put an IP address (potentially subject
    to a change) in his topology.yaml file instead of FQDN when available.
    * some relays have several IP addresses behind a domain name, however we
    currently use only one, as unfair as it might be for some relays, we
    believe on most configurations those different IPs belong to the same
    subnetwork and as such shouldn't present induce/warrant too big a change in
    error margin and most probably not in sorting order.
    * only one measure is taken for each node, in order to take the weather
    into acount (SYN tempest)
    * it can take some time to process every node, although this part could be
    mutlithreaded we try not to induce bias in results.
    * this tool will not open firewall ports for you in case you decide to
    update topology.yaml
    * obviously tests are to be conducted at different periods of time as
    network load and routes are constantly changing. It is only meant to be
    used as a tool and not as an «oracle». On that note it would be pretty
    straightforward to implement some daemon to monitor RTT for a few nodes,
    then plot a graph using the collected data.
'''

TOPOLOGY_JSON_URL = u'https://explorer.cardano-mainnet.iohk.io/relays/topology.json'
TOPOLOGY_FILENAME = u'topology.yaml'
# number of entries to be saved in topology.yaml (JSON ⊂ YAML)
NB_ROWS = 4

class TopoSorter:

    def __init__(self, *args, **kwargs):

        self.df = ''

    def do_http_get(self, url):
        try:
            r = rq.get(url)
            if r.status_code != 200:
                r.raise_for_status()
        except rq.HTTPError as e:
            print(e)
        except ConnectionError as e:
            print(e)
        return r

    def get_dataframe(self, url):

        j = self.do_http_get(url).json()
        #  print(j)
        p = j['Producers']

        #  print(f'{len(p)} elements in JSON')

        return pd.DataFrame(p, columns=['addr', 'port', 'continent', 'state', 'RTT'])

    def get_rtt(self, host, port):

        d = 255.0 # we chose a huge value to avoid «connection refused» bias
        print(f'Check RTT on {host} {port}/tcp')
        ''' we use the time needed to connect() as an approximation for RTT,
            we could have used another port and looked for ICMP Port
            Unreachable but we're lazy, and of course there is no guarantee
            that the port will not be filtered and the packet silently dropped
            by some evil (but useful) fw
        '''

        s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
        s.setsockopt(sock.SOL_SOCKET, sock.SO_REUSEADDR, 1)
        s.setsockopt(sock.IPPROTO_TCP, sock.TCP_NODELAY, 1)
        s.settimeout(3)
        try:

            start_time = tm.time()
            s.connect((host, port))
            end_time = tm.time()
            d = end_time - start_time

        except ConnectionRefusedError as e:
            print(f'\033[31m{host} {e}\033[0m')
        except IOError as e:
            print(f'\033[31m{host} NXDOMAIN OR timeout\033[0m')
        except Exception as e:
            print(f'\033[31m{host} Unknown exception {e}\033[0m')
        finally:
            s.close()

        return d


    def get_data(self, url):

        self.df = self.get_dataframe(url)

        ''' now we compute each RTT '''
        for row in self.df.itertuples():
            # row[0] is Index, so row[1] is host and row[2] is port
            self.df.at[row.Index, 'RTT'] = self.get_rtt(row[1], row[2])

        return self.df

    def sort_data(self):

        self.df.sort_values(by=['RTT'], inplace=True)

    def save_topo_file(self, fn, df):

        print(df)
        j = {}
        j['Producers'] = []
        for row in df.itertuples():
            j['Producers'].append({
                'addr' : row[1],
                'port' : row[2],
                'valency' : 1
                })

        with open(TOPOLOGY_FILENAME, 'w') as f:
            json.dump(j, f)

    def get_df(self):

        return self.df

    ''' check that we are indeed connected to the Internet,
        and try to gracefully handle ugly exceptions '''
    @staticmethod
    def is_connected():
        try:
            # connect to the host -- tells us if the host is actually reachable
            sk = sock.create_connection(('www.google.com', 80))
            if sk is not None:
                # closing socket
                sk.close()
            return True
        except OSError:
            pass
        return False


def main():

    if not TopoSorter.is_connected():
        sys.stderr.write('No Internet access, please try again.\n')
        sys.exit(-1)

    t = TopoSorter() # get an instance

    df = t.get_data(TOPOLOGY_JSON_URL) # fetch data

    t.sort_data() # now we sort by RTT

    print(f'\nSample of results:\n{t.get_df()}\n') # print sample

    # then we display the NB_ROWS most fastest results, following
    # topology.yaml style (JSON ⊂ YAML)
    t.save_topo_file(TOPOLOGY_FILENAME, t.get_df().iloc[:NB_ROWS])

    '''
        For Daedalus on GNU/Linux users, topology.yaml is in
        ~/.daedalus/nix/store/793w0g409m6x2kz7lq4bk3s2fjcljhh7-node-cfg-files/topology.yaml
        as the directory under store may vary, it is advised to locate it using
            $ sudo updatedb; locate topology.yaml
        check for consistency with
            $ cat topology.yaml |jq
    '''


if __name__ == '__main__':
    main()

