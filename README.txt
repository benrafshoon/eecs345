***NOTE*** The invocation of the program has changed from the specifications for project 1

Users of our service are be able to create, join, and leave messaging groups to send messages to a group of friends or peers. For our proof of concept implementation, we developed a simple command line interface to allow users to perform the basic Scribe group operations.

The command line program has the following flags:
    ­listen=ipaddress:port
        The ip address and port that the instance should use
    ­first­peer=ipaddress:port
        The ip address and port of another node in the system to contact first
    ­name=”A username”
        Your username

An example invocation with only one node might be:
    ./main ­listen=localhost:4000 ­first­peer=localhost:4000 ­name=”John Smith”


Once running, the following commands operate on a group:
    create_group group_name
        Creates a group with the given group name. The group name cannot contains
        spaces. Creating a group does not join the group.
    join_group group_name
        Joins a group with the given group name. Joining a group allows you to send
        messages to each node in the group and receive messages sent to the group.
    send_message group_name a message
        Sends a message to all members in the named group. Messages may contain
        spaces.
    leave_group group_name
        Leaves the group with the given group name. Leaving the message stops you from receiving messages sent to that group.


To test this, we suggest setting up several nodes, and have one node create a group.  Each node should join the group.  
Any node will be able to send a message to the group, and every node will receive messages sent by any node in groups
that it has joined.


See paper for implementation details