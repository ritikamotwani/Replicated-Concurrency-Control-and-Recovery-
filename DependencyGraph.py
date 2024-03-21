from collections import defaultdict


def is_cyclic_util(node, graph, visited, rec_stack):
    """
    dfs code to check for cycle
    """
    visited[node] = True
    rec_stack[node] = True

    for neighbor in graph[node]:
        if not visited[neighbor]:
            if is_cyclic_util(neighbor, graph, visited, rec_stack):
                return True
        elif rec_stack[neighbor]:
            return True

    rec_stack[node] = False
    return False


def is_cyclic(graph, nodes):
    """
    dfs code to check for cycle
    """
    visited = {node: False for node in nodes}
    rec_stack = {node: False for node in nodes}

    for node in nodes:
        if not visited[node]:
            if is_cyclic_util(node, graph, visited, rec_stack):
                return True

    return False


def has_consecutive_rw(edges):
    graph = defaultdict(list)
    for edge in edges:
        graph[edge[0]].append((edge[1], edge[2]))

    for start_node in graph:
        consecutive_rw_count = 0
        current_node = start_node

        while consecutive_rw_count < 2:
            neighbors = graph.get(current_node, [])

            rw_neighbors = [(neighbor, label) for neighbor, label in neighbors if label == 'rw']

            if rw_neighbors:
                consecutive_rw_count += 1
                current_node = rw_neighbors[0][0]
            else:
                break

        if consecutive_rw_count == 2:
            return True

    return False


class DependencyGraph:
    class TempNode:
        def __int__(self, transaction):
            pass

    class Node:
        def __init__(self, transaction):
            """
            Node constructor
            :param transaction: transaction as the node
            :param ww_edges: ww edges associated
            :param wr_edges: wr edges associated
            :param rw_edges: rw edges associated
            :param depends_on: node depends on what node, to get dependencies
            :param uncommitted_rw_dependencies: debugging param
            :param committed: debugging param
            """
            self.transaction = transaction
            self.ww_edges = []
            self.wr_edges = []
            self.rw_edges = []
            self.depends_on = set()
            self.uncommitted_rw_dependencies = []
            self.committed = False

        def __repr__(self):
            return "Node(transaction={}, depends_on=[{}])".format(self.transaction,
                                                                  ','.join([i.transaction for i in self.depends_on]))

    def __init__(self):
        self.nodes = {}
        self.edges = set()

    def add_rw_dependency(self, t1, t2):
        """
        Unused
        """
        # print("Adding r->w dependency for: ", t1, t2)
        if t1 not in self.nodes:
            self.nodes[t1] = self.Node(t1)
        if t2 not in self.nodes:
            self.nodes[t2] = self.Node(t2)
        self.nodes[t1].depends_on.add(self.nodes[t2])

    def will_create_cycle(self, transaction, logs_by_var, transactions_map):
        """
        The inputs comprise of the current transaction that 
        has called the will_create_cycle function to check if it
        can abort without forming a cycle. The logs by variable and the transaction map (name: Transaction).
        It first creates a node for the current transaction, then we have a loop for adding edges.
        It iterates over the logs by variables. log of logs of variables. If the log is associated with the current transaction then
        has_current_transaction_began is set to True and then we check the operation of the current transaction. 
        Else if the other transaction has a read operation start appending to the read_write dependency array.
        After the inner loop is done, we check if not has_current_transaction_began or (not current_transaction_write)
        to see the rw dependency added is really a dependency or not If not, we clear the list.
        In a similar way we check for the ww dependencies. 
        In the set of edges we add all the rw and ww dependencies with a label rw and ww. Then we create a graph.
        We use adjacency list for it. Then to check the cycle we have these two steps:
        Step 1. Check if the graph has two consecutive rw dependencies, then a deadlock cycle should be removed.
        Step 2. Check if the new transaction causes a cycle in the graph and whether it should be aborted.
        """
        # temporarily add this to the graph
        transaction_node = self.Node(transaction)
        # print("Checking for cycles in transaction={}, graph={}, edges={}".format(transaction, self.nodes, self.rw_edges))

        # add edges
        for var, logs in logs_by_var.items():
            prior_transactions = []
            rw_dependencies = []
            current_transaction_write = False
            current_transaction_read = False
            has_current_transaction_began = False
            # print(var, logs)
            for log in logs:
                # print(log.transaction_identifier, transaction, log.op, has_current_transaction_began, current_transaction_write)

                if log.transaction_identifier == transaction:
                    has_current_transaction_began = True
                    if log.op == "write":
                        # print("DE", log.transaction_identifier, transaction, log.op)
                        current_transaction_write = True
                    elif log.op == "read":
                        current_transaction_read = True
                else:
                    # if log.transaction_identifier in self.nodes:
                    #     # print("{} is already committed, won't have RW dependency".format(log.transaction_identifier))
                    #     # this transaction is committed
                    #     if has_current_transaction_began:
                    #         # doesn't match any case
                    #         continue
                    #     else:
                    #         prior_transactions.append((log.transaction_identifier, log.op))
                    # else:
                    #     # rw case -- transaction is uncommitted
                    #     pass
                    if log.op == "read":
                        # it's happening before the current transaction ends
                        # current transaction may not have written yet.
                        # print("RW dependency detected {}->{} via {}".format(log.transaction_identifier, transaction, var))
                        rw_dependencies.append(log.transaction_identifier)
            if not has_current_transaction_began or (not current_transaction_write):
                # print("Clearing rw dependencies, because current transaction {} has no write.".format(transaction))
                rw_dependencies = []

            for prior_transaction in prior_transactions:
                if prior_transaction[1] == "write":
                    if current_transaction_read:
                        # Add wr dependency
                        self.nodes[prior_transaction[0]].wr_edges.append(transaction_node)
                    if current_transaction_write:
                        # Add ww dependency
                        self.nodes[prior_transaction[0]].ww_edges.append(transaction_node)

            # both nodes in a rw dependency are uncommitted
            # when trying to commit,
            for rw_dependency in rw_dependencies:
                self.edges.add((rw_dependency, transaction_node.transaction, 'rw'))

        # check for ww dependencies
        for var, logs in logs_by_var.items():
            write_transactions_to_var = []
            current_transaction_has_write = False
            for log in logs:
                if log.transaction_identifier == transaction and log.op == "write":
                    current_transaction_has_write = True
                elif log.transaction_identifier != transaction and log.op == "write":
                    # if any transaction is in this list, and in graph, it's already committed.
                    # if T commits before T' begins, and both write to x, then T->ww->T'
                    if log.transaction_identifier in self.nodes and transactions_map[log.transaction_identifier].committed_at < transactions_map[transaction].start_time:
                        write_transactions_to_var.append(log.transaction_identifier)
            for t in write_transactions_to_var:
                self.edges.add((t, transaction, 'ww'))
            # print("ww-dependency{}=>{}".format(write_transactions_to_var, transaction))
        # print(self.edges)
        has_consecutive_rw_result = has_consecutive_rw(self.edges)
        # print(has_consecutive_rw_result)
        if has_consecutive_rw_result:
            graph = defaultdict(list)
            for edge in self.edges:
                graph[edge[0]].append(edge[1])
            # print(graph, self.edges)
            nodes = set(node for edge in self.edges for node in edge)
            if is_cyclic(graph, nodes):
                print("The graph has a cycle.")
                # cycle detected
                # don't commit
                return True
        self.nodes[transaction_node.transaction] = transaction_node
        # print(self.nodes, self.rw_edges, matrix)
        return False

    def clear(self):
        """
        Unused
        """
        self.nodes.clear()


dependency_graph = DependencyGraph()
