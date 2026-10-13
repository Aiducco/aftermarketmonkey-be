class TheWheelGroupException(Exception):
    pass


class TheWheelGroupDownloadError(TheWheelGroupException):
    pass


class TheWheelGroupParseError(TheWheelGroupException):
    pass


class TheWheelGroupFileNotFoundError(TheWheelGroupException):
    pass


class TheWheelGroupSFTPConnectionError(TheWheelGroupException):
    def __init__(self, message: str) -> None:
        TheWheelGroupException.__init__(self)
        self.message = message


class TheWheelGroupFeedNotYetDeliveredException(TheWheelGroupException):
    """
    Raised by sync_the_wheel_group_company_pricing_for_company_provider when this company has no
    relay file yet and the sync fell back to the public mastersheet — a normal, expected, not an
    error state (see that function's own docstring). Caught specifically in
    integration_pricing_sync_jobs.run_integration_pricing_sync_job so a job in this state is
    deferred rather than marked COMPLETED (which used to flip initial_sync_completed/CONNECTED on
    the strength of the fallback alone — confirmed live 2026-10-13 on two real connections,
    SunCoast and Trident, both showing CONNECTED with zero relay files ever delivered) or FAILED
    (which would log an alarming traceback for a state that is not a failure at all).
    """
    pass
