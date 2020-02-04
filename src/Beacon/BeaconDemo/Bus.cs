using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using NetMQ;
using NetMQ.Sockets;

namespace BeaconDemo
{
    /// <summary>
    /// Originally from the Docs\beacon.md documentation with small modifications for this demo
    /// </summary>
    internal class Bus
    {
        // Actor Protocol
        public const string PublishCommand = "P";
        public const string GetHostAddressCommand = "GetHostAddress";
        public const string AddedNodeCommand = "AddedNode";
        public const string RemovedNodeCommand = "RemovedNode";

        // Dead nodes timeout
        private readonly TimeSpan m_deadNodeTimeout = TimeSpan.FromSeconds(10);

        // we will use this to check if we already know about the node
        private class NodeKey
        {
            public NodeKey(string name, int port)
            {
                Name = name;
                Port = port;
                Address = $"tcp://{name}:{port}";
                HostName = Dns.GetHostEntry(name).HostName;
            }

            public string Name { get; }
            public int Port { get; }
            public string Address { get; }
            public string HostName { get; private set; }

            protected bool Equals(NodeKey other)
            {
                return string.Equals(Name, other.Name) && Port == other.Port;
            }

            public override bool Equals(object obj)
            {
                if (obj == null) return false;

                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != this.GetType()) return false;
                return Equals((NodeKey)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return ((Name?.GetHashCode() ?? 0) * 397) ^ Port;
                }
            }

            public override string ToString()
            {
                return Address;
            }
        }

        private readonly int broadcastPort;

        private readonly NetMQActor actor;

        private PublisherSocket publisher;
        private SubscriberSocket subscriber;
        private NetMQBeacon beacon;
        private NetMQPoller poller;
        private PairSocket shim;
        private readonly Dictionary<NodeKey, DateTime> nodes; // value is the last time we "saw" this node
        private int randomPort;

        private Bus(int broadcastPort)
        {
            nodes = new Dictionary<NodeKey, DateTime>();
            this.broadcastPort = broadcastPort;
            actor = NetMQActor.Create(RunActor);
        }

        /// <summary>
        /// Creates a new message bus actor. All communication with the bus is
        /// through the returned <see cref="NetMQActor"/>.
        /// </summary>
        public static NetMQActor Create(int broadcastPort)
        {
            var node = new Bus(broadcastPort);
            return node.actor;
        }

        private void RunActor(PairSocket shim)
        {
            // save the shim to the class to use later
            this.shim = shim;

            // create all subscriber, publisher and beacon
            using (subscriber = new SubscriberSocket())
            using (publisher = new PublisherSocket())
            using (beacon = new NetMQBeacon())
            {
                // listen to actor commands
                this.shim.ReceiveReady += OnShimReady;

                // subscribe to all messages
                subscriber.Subscribe("");

                // we bind to a random port, we will later publish this port
                // using the beacon
                randomPort = subscriber.BindRandomPort("tcp://*");
                Console.WriteLine("Bus subscriber is bound to {0}", subscriber.Options.LastEndpoint);

                // listen to incoming messages from other publishers, forward them to the shim
                subscriber.ReceiveReady += OnSubscriberReady;

                // configure the beacon to listen on the broadcast port
                Console.WriteLine("Beacon is being configured to UDP port {0}", broadcastPort);
                beacon.Configure(broadcastPort);

                // publishing the random port to all other nodes
                Console.WriteLine("Beacon is publishing the Bus subscriber port {0} every second", randomPort);
                beacon.Publish(randomPort.ToString(), TimeSpan.FromSeconds(1));

                // Subscribe to all beacon on the port
                Console.WriteLine("Beacon is subscribing to all beacons on UDP port {0}", broadcastPort);
                beacon.Subscribe("");

                // listen to incoming beacons
                beacon.ReceiveReady += OnBeaconReady;

                // Create a timer to clear dead nodes
                var timer = new NetMQTimer(TimeSpan.FromSeconds(1));
                timer.Elapsed += ClearDeadNodes;

                // Create and configure the poller with all sockets and the timer
                poller = new NetMQPoller { this.shim, subscriber, beacon, timer };

                // signal the actor that we finished with configuration and
                // ready to work
                this.shim.SignalOK();

                // polling until cancelled
                poller.Run();
            }
        }

        private void OnShimReady(object sender, NetMQSocketEventArgs e)
        {
            // new actor command
            var command = shim.ReceiveFrameString();

            // check if we received end shim command
            if (command == NetMQActor.EndShimMessage)
            {
                // we cancel the socket which dispose and exist the shim
                poller.Stop();
            }
            else if (command == PublishCommand)
            {
                // it is a publish command
                // we just forward everything to the publisher until end of message
                var message = shim.ReceiveMultipartMessage();
                publisher.SendMultipartMessage(message);
            }
            else if (command == GetHostAddressCommand)
            {
                var address = beacon.BoundTo + ":" + randomPort;
                shim.SendFrame(address);
            }
        }

        private void OnSubscriberReady(object sender, NetMQSocketEventArgs e)
        {
            // we got a new message from the bus
            // let's forward everything to the shim
            var message = subscriber.ReceiveMultipartMessage();
            shim.SendMultipartMessage(message);
        }

        private void OnBeaconReady(object sender, NetMQBeaconEventArgs e)
        {
            // we got another beacon
            // let's check if we already know about the beacon
            var message = beacon.Receive();
            int port;
            int.TryParse(message.String, out port);

            NodeKey node = new NodeKey(message.PeerHost, port);

            // check if node already exist
            if (!nodes.ContainsKey(node))
            {
                // we have a new node, let's add it and connect to subscriber
                nodes.Add(node, DateTime.Now);
                publisher.Connect(node.Address);
                shim.SendMoreFrame(AddedNodeCommand).SendFrame(node.Address);
            }
            else
            {
                //Console.WriteLine("Node {0} is not a new beacon.", node);
                nodes[node] = DateTime.Now;
            }
        }

        private void DumpNodes(DateTime now, string title)
        {
            Console.WriteLine("---------------------------------------------");
            Console.WriteLine($"{nodes.Count} nodes ({title}):");
            foreach (var key in nodes.Keys)
            {
                var datetime = nodes[key];
                var isDead = now > datetime + m_deadNodeTimeout;
                Console.WriteLine($"name={key.Name}, host={key.HostName}, port={key.Port}, dead={isDead}");
            }
        }

        private void ClearDeadNodes(object sender, NetMQTimerEventArgs e)
        {
            //Console.WriteLine("");
            //Console.WriteLine($"ClearDeadNodes");

            var now = DateTime.Now;
            //DumpNodes(now, "BeforeClearDateNodes");

            // create an array with the dead nodes
            var deadNodes = nodes.Where(n => now > n.Value + m_deadNodeTimeout)
                                 .Select(n => n.Key)
                                 .ToArray();

            // remove all the dead nodes from the nodes list and disconnect from the publisher
            foreach (var node in deadNodes)
            {
                nodes.Remove(node);
                publisher.Disconnect(node.Address);
                shim.SendMoreFrame(RemovedNodeCommand).SendFrame(node.Address);
            }

            //DumpNodes(now, "AfterClearDateNodes");
        }
    }
}
