from datetime import date


class SnapshottedDimensionPath(str):
    """
    Marker class for the snapshotted dimension path context.
    """

    pass


class OutputPath(str):
    """
    Marker class for the output path context.
    """

    pass


class RunDate(date):
    """
    Marker class for the run date context.
    """

    pass
