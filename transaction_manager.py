from VirtualClock import virtual_clock


class TransactionManager:
    class Transaction:
        class TransactionLogEntry:
            WRITE = "write"
            READ = "read"
            BEGIN = "begin"

            def __init__(self, transaction_identifier, op, variable=None, value=None):
                """
                TransactionLogEntry constructor
                :param op: The operation (read, write, begin) in the transaction log.
                :param variable: The variable that the transaction log is working with.
                :param value: The value with the transaction log's operation
                :param transaction_identifier: transaction name
                :param timestamp: timestamp associated with the log.
                """
                self.op = op
                self.variable = variable
                self.value = value
                self.transaction_identifier = transaction_identifier
                self.timestamp = virtual_clock.get_time()

            def __repr__(self):
                return "Log({}{}{})".format(self.transaction_identifier, self.variable, self.op)
        def __init__(self, name, last_seen_commits):
            """
            Transaction constructor
            :param name: transaction name
            :param last_seen_commits: latest write commit by every variable
            :param state: whether active or committed
            :param start_time: start time of a transaction
            :param committed_at: when the transaction was committed
            :param log: logs (of type TransactionLogEntry) list of a transaction
            """
            self.name = name
            self.last_seen_commits = last_seen_commits
            self.state = "ACTIVE"
            self.start_time = virtual_clock.get_time()
            self.committed_at = None
            self.log = []

        def log_write(self, variable, value):
            """
            Append a log entry to the log list parameter of Transaction. The log entry constructor requires transaction name,
            the operation which is write in this case, variable to write to with the value that has to be written.
            """
            self.log.append(self.TransactionLogEntry(self.name, self.TransactionLogEntry.WRITE, variable, value))

        def log_read(self, variable):
            """
            Append a log entry to the log list parameter of Transaction. The log entry constructor requires transaction name,
            the operation which is read in this case, and the variable that has to be read.
            """
            self.log.append(self.TransactionLogEntry(self.name, self.TransactionLogEntry.READ, variable))

        def log_begin(self):
            """
            Append a log entry to the log list parameter of Transaction. The log entry constructor requires transaction name,
            the operation which is begin in this case.
            """
            self.log.append(self.TransactionLogEntry(self.name, self.TransactionLogEntry.BEGIN))

        def __repr__(self):
            return "Transaction(name={}, state={}, last_seen_commits={})".format(self.name, self.state,
                                                                                 self.last_seen_commits)

    def __init__(self, data_manager):
        """
        Transaction constructor
        :param data_manager: The data_manager class object associated here with this class object.
        :param active_transactions: when the transaction begins add all transactions here in the dict.
        :param states: unused debugging var
        """
        self.data_manager = data_manager
        self.active_transactions = {}
        self.states = {}

    def get_transaction_states(self):
        """
        For every active transaction it adds the logs to the states.
        It is required to call the transaction commit function.
        """
        states = {}
        for transaction in self.active_transactions:
            states[transaction] = self.active_transactions[transaction].log
        return states

    def handle_begin_transaction(self, transaction):
        """
        Begin transaction function. It adds the new transaction to active_transactions.
        And this in turn calls the data manager with the register_transaction_begin function.
        """
        self.active_transactions[transaction] = self.Transaction(transaction, self.data_manager.get_last_commits())
        self.active_transactions[transaction].log_begin()
        self.data_manager.register_transaction_begin(self.active_transactions[transaction])
        # print(self.active_transactions)
        # self.data_manager.register_transaction_begin(transaction)

    def handle_end_transaction(self, transaction):
        """
        End transaction function. It calls the attemp transaction function which checks if the transaction
        should be committed or aborted. If the outcome to commit is True, the transaction state is made COMMITTED.
        Else it prints the abort transaction part.
        """
        outcome, committed_version = self.data_manager.attempt_transaction_commit(self.active_transactions[transaction], self.get_transaction_states())
        if outcome:
            self.active_transactions[transaction].state = "COMMITTED"
            self.active_transactions[transaction].committed_at = virtual_clock.get_time()

            print("Transaction {} successful".format(transaction))
        else:
            print(
                "Transaction {} aborted because of conflict, {}".format(transaction, committed_version))

    def handle_read(self, transaction, variable):
        """
        1. get active transactions.
        2. Check if some active uncommitted transaction has written this variable prior to it, if yes, it's a rw dependency from
        that transaction to this one.
        :return: None
        """
        self.active_transactions[transaction].log_read(variable)
        self.data_manager.register_transaction_read(self.active_transactions[transaction], variable)

    def handle_write(self, transaction, var, val):
        """
        1. get active transactions.
        2. Add logs which will later help to check ww edges.
        3. register the write with database manager class that helps to add transaction snapshots, helping check write first logic.
        """
        self.active_transactions[transaction].log_write(var, val)
        self.data_manager.register_transaction_write(transaction, var, val)
        # print(self.data_manager.variables)

    def handle_dump(self):
        pass
