from datetime import date


class OrdersPath(str):
    """
    Marker class for the orders path context.
    """

    pass


class CustomersPath(str):
    """
    Marker class for the customers path context.
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
