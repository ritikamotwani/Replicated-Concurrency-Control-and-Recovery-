import collections
from typing import Optional

from DependencyGraph import dependency_graph
from VirtualClock import virtual_clock
from transaction_manager import TransactionManager


class DataManager:
    STATUS_UP = "UP"
    STATUS_DOWN = "DOWN"

    class Var:  # per site
        def __init__(self, idx, sites=[]):
            """
            Var constructor
            :param idx: variable number as index.
            :param name: x + str(idx)
            :param uncommitted_vals: Value of the variable within a transaction before the transaction ends.
            :param committed_version: version. initial or the transaction name.
            :param sites: List of sites that have this variable.
            :param last_write_success: Was the last write for the variable successful.
            :param read_blocked: Is read blocked for the variable.
            """
            self.idx = idx
            self.name = "x" + str(idx)
            self.uncommitted_vals = {}
            self.committed_version = "initial"  # values can only be committed by the initializer, or by transactions
            self.sites = []
            self.last_write_success = False
            self.read_blocked = False

        def read_var(self, transaction) -> Optional[int]:
            """
            Reads the var from the site(s)
            :return: value of the var
            For odd variables:
            Upon recovery of a site s, all non-replicated variables are available for
                reads and writes.
            For even variables:
            Regarding replicated variables, the site makes them available for writing,
                but not reading for transactions that begin after the recovery until a commit
                has happened. In fact, a read from a transaction that begins after the recovery
                of site s for a replicated variable x will not be allowed at s until a committed 
                write to x takes place on s
            """

            if self.idx % 2 == 1:
                """
                Upon recovery of a site s, all non-replicated variables are available for
                reads and writes.
                """
                site = self.sites[0]
                if site.status == DataManager.STATUS_UP or (site.recovery_history[-1] < transaction.start_time 
                                                            and transaction.start_time < site.failure_history[-1]):
                    # as long as the site s up for single replica case, it doesn't matter and it will return
                    # or, if transaction began before the first failure
                    return site.vars[self.name]['transaction_snapshots'][transaction.name][0]
                
            else:
                """
                Regarding replicated variables, the site makes them available for writing,
                but not reading for transactions that begin after the recovery until a commit
                has happened. In fact, a read from a transaction that begins after the recovery
                of site s for a replicated variable x will not be allowed at s until a committed 
                write to x takes place on s
                """
                for site in self.sites:
                    last_recovered = site.recovery_history[-1]
                    last_failed = site.failure_history[-1] if site.failure_history else 0
                    if site.status == DataManager.STATUS_UP:
                        if last_failed < transaction.start_time and last_recovered < transaction.start_time:
                            # print(transaction.name, transaction.start_time, last_recovered, last_failed, site.idx, site.vars[self.name]['committed_at'], self.name)
                            # print(site.vars[self.name]['committed_at'], last_recovered, last_failed)
                            if site.vars[self.name]['committed_at'] > last_recovered and (site.vars[self.name]['committed_at'] < transaction.start_time or last_failed == 0):
                                return site.vars[self.name]['transaction_snapshots'][transaction.name][0]
                        continue
            print("Read failed as none of the sites hosting this var are up")
            print("{} will abort if not unblocked by recovery of any site".format(transaction.name))
            for site in self.sites:
                temp = site.vars[self.name]['transaction_snapshots'][transaction.name]
                site.vars[self.name]['transaction_snapshots'][transaction.name] = (temp[0], temp[1], temp[2], temp[3], True)
                # print(site.vars[self.name])
            # transaction.read_even_blocked = True
            return None

        def write_var(self, transaction, val):
            """
            Writes the var
            The function iterates over all sites. If the site status is up, it finds the variable that has to be updated,
            we update the transaction snapshot for it. The flow is site -> vars -> var name -> transaction snapshot -> (value, 
            whether site was updated since T began, update time, update attempt time, read blocked for that transaction).
            We cannot commit the write before end, so we just update the transaction snapshot for that variable. In the transaction snapshot,
            the new values is added, site updated gets True, add the time for when the write happens, add the time when the attempt is done, and if the read is blocked for that var.
            If the site is down, in the transaction snapshot everything else remains same as previous, we only change the timer of whether attempt to update was done.
            The whether update is done to check if we try to write on a failed site and later check whether to abort the transaction for that.
            If atleast one successful write was made we make the write success as True else print an error.
            """
            self.last_write_success = False
            success_count = 0
            for site in self.sites:
                # print(site.status)

                if site.status == DataManager.STATUS_UP:
                    # value, whether site was updated since T began, update time, update attempt time, read blocked for that transaction
                    temp = site.vars[self.name]['transaction_snapshots'][transaction]
                    site.vars[self.name]['transaction_snapshots'][transaction] = (val, True, virtual_clock.get_time(), virtual_clock.get_time(), temp[4])
                    success_count += 1
                else:
                    temp = site.vars[self.name]['transaction_snapshots'][transaction]
                    site.vars[self.name]['transaction_snapshots'][transaction] = (temp[0], temp[1], temp[2], virtual_clock.get_time(), temp[4])
                    # print(site.vars[self.name]['transaction_snapshots'][transaction])
            if success_count >= 1:  # write should succeed for at-least one site
                self.last_write_success = True
            else:
                print("Transaction commit will fail as only {}/{} sites were up".format(success_count, len(self.sites)))
            return self.last_write_success

        def __repr__(self):
            """
            Debugging logs
            """
            return "x{}(sites={}, uncommitted_vals={}, committed_version={}".format(self.idx, self.sites,
                                                                                    self.uncommitted_vals,
                                                                                    self.committed_version)

    class Site:
        def __init__(self, idx, status):
            """
            Site constructor
            :param idx: site number as index.
            :param status: whether site status is up or down.
            :param vars: all the variables on that site. For every variable we have this associated to it{"val": var.idx * 10, "committed_at": virtual_clock.get_time(), "uncommitted_at": None,
                                       "transaction_snapshots": {}}
            :param recovery_history: when was the site last recovered. Initially all sites are recovered at the start time.
            :param failure_history: add the time to this list when the site failed.
            """
            self.idx = str(idx)
            self.status = status
            self.vars = {}
            self.recovery_history = [virtual_clock.get_time()]
            self.failure_history = []

        def __repr__(self):
            """
            Debugging logs
            """
            return "Site(idx={}, status={}, vars={})".format(self.idx, self.status, self.vars)

        def fail(self):
            """
            Site class fail method. When the site fails we add that time to the failure history list of that particular site.
            The status of site is changed to down.
            """
            self.failure_history.append(virtual_clock.get_time())
            self.status = DataManager.STATUS_DOWN

        def recover(self):
            """
            Site class recover method. When the site recovers we add that time to the recover history list of that particular site.
            The status of site is changed to up.
            """
            self.recovery_history.append(virtual_clock.get_time())
            self.status = DataManager.STATUS_UP

    def __init__(self):
        """
        DataManager constructor
        :param x...: All variables are initialised in the main  datamanager class with the type as class Var
        :param s..: All sites are initialised here as class Site type. The initial state is UP for all sites.
        :param sites: List of all initialised sites.
        :param variables: List of all initialised variables.
        :param variables_map: dictionary of variable name with initalised correct variable.
        :param sites_map: dictionary of site index with initalised sites.
        :param transactions_map: In the datamanager class have a dictionary of all transaction names with the transaction
        """
        self.x1 = self.Var(1)
        self.x2 = self.Var(2)
        self.x3 = self.Var(3)
        self.x4 = self.Var(4)
        self.x5 = self.Var(5)
        self.x6 = self.Var(6)
        self.x7 = self.Var(7)
        self.x8 = self.Var(8)
        self.x9 = self.Var(9)
        self.x10 = self.Var(10)
        self.x11 = self.Var(11)
        self.x12 = self.Var(12)
        self.x13 = self.Var(13)
        self.x14 = self.Var(14)
        self.x15 = self.Var(15)
        self.x16 = self.Var(16)
        self.x17 = self.Var(17)
        self.x18 = self.Var(18)
        self.x19 = self.Var(19)
        self.x20 = self.Var(20)
        self.s1 = self.Site(1, self.STATUS_UP)
        self.s2 = self.Site(2, self.STATUS_UP)
        self.s3 = self.Site(3, self.STATUS_UP)
        self.s4 = self.Site(4, self.STATUS_UP)
        self.s5 = self.Site(5, self.STATUS_UP)
        self.s6 = self.Site(6, self.STATUS_UP)
        self.s7 = self.Site(7, self.STATUS_UP)
        self.s8 = self.Site(8, self.STATUS_UP)
        self.s9 = self.Site(9, self.STATUS_UP)
        self.s10 = self.Site(10, self.STATUS_UP)

        self.sites = [self.s1, self.s2, self.s3, self.s4, self.s5, self.s6, self.s7, self.s8, self.s9, self.s10]

        self.variables = [self.x1, self.x2, self.x3, self.x4, self.x5, self.x6, self.x7, self.x8, self.x9, self.x10,
                          self.x11, self.x12, self.x13, self.x14, self.x15, self.x16, self.x17, self.x18, self.x19,
                          self.x20]
        self.variables_map = {v.name: v for v in self.variables}
        self.sites_map = {s.idx: s for s in self.sites}
        self.transactions_map = {}

    def initialize(self):
        """
        Iterate over every variable in the parameter of the datamanager class.
        For every variable send the variable index to the getsite function 
        and find the site where the variable is at.
        For the sites returned (all in case of even, 1 in case of odd) for every site.vars[var.name]
        initialise with the value (index * 10), committed_at: cuurent time, uncommitted_at: None
        and an empty dictionary for transaction snapshots as there are no snapshots currently, for any variable.
        This is just an initialisation function to start before reading teh transactions.
        """
        for var in self.variables:
            var.sites = self.get_sites(var.idx)
            for site in var.sites:
                site.vars[var.name] = {"val": var.idx * 10, "committed_at": virtual_clock.get_time(), "uncommitted_at": None,
                                       "transaction_snapshots": {}}

    def get_sites(self, idx):
        """
        If index is even then the get site has to return all sites.
        If index is odd then only one site has that variable, so return the correct site.
        """
        if idx % 2 == 0:
            return self.sites
        else:
            return [self.sites[idx % 10]]

    def handle_fail_site(self, site):
        # print(self.sites_map)
        """
        When a site fails the site class' fail method is called.
        """
        self.sites_map[site].fail()

    def dump(self):
        """
        The dump function iterates over every site and 
        prints the variables and the variable values associated with that site.
        """
        for site in self.sites:
            print("Site {} - ".format(site.idx), end='')
            print(", ".join(var + ": " + str(details['val']) for var, details in site.vars.items()))


    def handle_recover_site(self, site):
        """
        This function first calls the recover method associated with the site class.
        Then it iterates over all sites to find what transactions have been read blocked because of 
        no site being up (even variables) and unblocks them to avoid aborting the transaction if they have not ended
        """
        self.sites_map[site].recover()
        # unblock all even variables reads that were blocked because of all failed sites
        change = False
        for site in self.sites:
            for _, var in site.vars.items():
                for transaction in var['transaction_snapshots']:
                    temp = var['transaction_snapshots'][transaction]
                    var['transaction_snapshots'][transaction] = (temp[0], temp[1], temp[2], temp[3], False)
                    change = True
        if change:
            print("The blocked read is now unblocked for transaction")

    def register_transaction_write(self, transaction, varName, value):
        # self.variables_map[varName].uncommitted_vals["uncommitted_" + transaction] = value
        """
        This function first calls the write variable function associated with the variable class.
        """
        self.variables_map[varName].write_var(transaction, value)

    def register_transaction_read(self, transaction, varName):
        """
        It prints the value read by the transaction when executing.
        """
        print("Read value result: {}".format(self.variables_map[varName].read_var(transaction)))
    
    def register_transaction_begin(self, transaction):
        """
        It is called when the transaction begins. 
        In the transaction map we add the new transaction name and the class object.
        Iterate over all the sites and if the site status is UP, we iterate over all variables at that site.
        To every variable we initialise the transaction snapshot object. We add the tuple:
        (value, False(not writing), time, time, False(reading not blocked)). If the site is down then
        the tuple becomes (None(no val as site is down), False(not writing), None, None, False(reading not blocked))
        """
        self.transactions_map[transaction.name] = transaction
        for _, site in self.sites_map.items():
            if site.status == DataManager.STATUS_UP:
                for _, var in site.vars.items():
                    var['transaction_snapshots'][transaction.name] = (var['val'], False, virtual_clock.get_time(), virtual_clock.get_time(), False)
            else:
                for _, var in site.vars.items():
                    var['transaction_snapshots'][transaction.name] = (None, False, None, None, False)

    def get_last_commits(self):
        """
        Get latest commit variable commit version.
        """
        var_commits = {}
        for var in self.variables:
            if var.committed_version != 'initial':
                var_commits[var.name] = var.committed_version.name
            else:
                var_commits[var.name] = var.committed_version
        return var_commits

    def get_logs_by_var(self, transaction_logs):
        """
        Get logs variable level for every transaction. The logs are sorted by timestamp. 
        They will be needed to detect a cycle in the graph of transactions.
        """
        var_level_logs = collections.defaultdict(list)
        for transaction, logs in transaction_logs.items():
            for log in logs:
                if log.variable is not None:
                    var_level_logs[log.variable].append(log)

        for var, logs in var_level_logs.items():
            var_level_logs[var] = sorted(logs, key=lambda x: x.timestamp)
        return var_level_logs

    def _parse_transaction_logs(self, transaction_logs, transaction_passed):
        """
        Unused function for debugging.
        """
        var_level_logs = collections.defaultdict(list)
        for transaction, logs in transaction_logs.items():
            for log in logs:
                var_level_logs[log.variable].append(log)

        for var, logs in var_level_logs.items():
            var_level_logs[var] = sorted(logs, key=lambda x: x.timestamp)

        ## TODO: separate this out to a new function
        for var, logs in var_level_logs.items():
            read_detected_transaction = None
            for log in logs:
                if log.op == "read":
                    read_detected_transaction = log.transaction_identifier
                elif log.op == "write":
                    if read_detected_transaction:
                        # print("adding dependency")
                        dependency_graph.add_rw_dependency(read_detected_transaction, log.transaction_identifier)
                        read_detected_transaction = None
        return dependency_graph.is_acyclic(transaction_passed)






    def attempt_transaction_commit(self, transaction: TransactionManager.Transaction, transaction_logs):
        """
        When end Transaction happens this function is called.
        We find if because of any conflict the transaction should be aborted.
        Iterate over all variables. For every variable iterate over the sites it is on.
        Cases:
        Case 1. For the conflict about site failing after a transaction attempts to write to it and ends after the fail.
        We check for every failure of the site and 
        if it happened after the transaction attempted to write to it, we abort the transaction.
        Case 2. We first check if the current transaction snapshot for that variable has an associated write.
        That means that the current transaction has attempted to write on that variable in its course.
        we check if for the variable the last seen commit is same as the variable's committed version of the transaction.
        If it is not that means another transaction committed to it before it could. So by the logic of first committer wins
        the transaction is aborted.
        Case 3. We check if in the transaction snapshot for a variable on a site the read got blocked because 
        of failed sites, and even if it recovered no one wrote to it, we abort the transaction.

        Then we exit the loop. For the last case we have to check if the serialization graph has a cycle.
        Case 4. We call the dependency graph class function to check the cycle and pass the transaction name, logs and map.
        If will create cycle function returns True we abort the transaction with the cycle reason.

        If none of the cases are True, we update the value of the variable, the committed at becomes the new time
        and the committed version of the variable has the current transaction name. 
        """
        outcome = True
        conflicts = []

        # if transaction.read_even_blocked:
        #     return False, ["Aborted because no site has a committed write to read the variable being read"]

        for v in self.variables:
            for site in v.sites:
                for failure in site.failure_history:
                    if site.vars[v.name]['transaction_snapshots'][transaction.name][1] and failure > site.vars[v.name]['transaction_snapshots'][transaction.name][3]:
                        # print(failure, v.name, site.vars[v.name]['transaction_snapshots'][transaction.name])
                        return False, ['site failed after a write']
                if site.vars[v.name]['transaction_snapshots'][transaction.name][1]:
                    if v.committed_version != 'initial' and transaction.last_seen_commits[v.name] == v.committed_version.name:
                        continue
                    else:
                        # print(transaction.name, v.name, v.committed_version)
                        if v.committed_version != 'initial' and not v.committed_version.committed_at < transaction.start_time:
                            outcome = False
                            conflicts.append((v.name, v.committed_version.name, 'committed first'))
                if site.vars[v.name]['transaction_snapshots'][transaction.name][4]:
                    return False, ["Aborted because no site has a committed write to read the variable being read"]
        
        if not outcome:
            return False, conflicts

        else:
            
            # # check for rw cycles
            # """
            # get all operations by variable
            # if read in Tx, and written in Ty then T1 --RW--> T2
            # """
            # if not self._parse_transaction_logs(transaction_logs, transaction.name):
            #     return False, ["Conflict due to RW cyclic dependencies."]

            # update graph here
            if dependency_graph.will_create_cycle(transaction.name, self.get_logs_by_var(transaction_logs), self.transactions_map):
                return False, ["Aborting; because it would have created a cycle"]
            
            for v in self.variables:
                # transaction_uncommited_val = v.uncommitted_vals.get("uncommitted_" + transaction.name, None)
                for site in v.sites:
                    if site.vars[v.name]['val'] != site.vars[v.name]['transaction_snapshots'][transaction.name][0] and site.vars[v.name]['transaction_snapshots'][transaction.name][1]:
                        site.vars[v.name]['val'] = site.vars[v.name]['transaction_snapshots'][transaction.name][0]
                        site.vars[v.name]['committed_at'] = virtual_clock.get_time()
                        v.committed_version = transaction
                        # print(v.name,transaction.name, site.vars[v.name])

            return True, conflicts


database = DataManager()
database.initialize()
