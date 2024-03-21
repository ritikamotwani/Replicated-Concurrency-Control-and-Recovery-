class VirtualClock:
    """
    All test-cases are sequential in nature, and there is no concurrent operation.
    Therefore an application level clock can be maintained, which simulates time
    """
    def __init__(self):
        self.time = 0

    def get_time(self):
        self.time += 1
        return self.time


virtual_clock = VirtualClock()