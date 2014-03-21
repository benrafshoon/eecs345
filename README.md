eecs345
=======

Cassie 	- I added the heartbeat system here. Nodes periodically check in on their parents. If their parent is down they will rejoin the group. 
	- I attempted to add total ordering. Each group table also has a list of messages that has been received by the node. The leader is in charge
	of assigned a numeric value to each message before it broadcasts it down the tree. The idea was that the leader should asks its children if
	any of them have a higher order message before sending it. Something weird is going on where the request to the child is never returned. I've
	been looking at it for a couple of hours and I'm very tired and have no more ideas. 
	The second phase would use the exact same function. Whenever a node receives a message it checks to see if it has a gap in messages. For example,
	a node will receive a message with order 7 but its last message was order 4. The node would then ask its parent for all the previous messages.
