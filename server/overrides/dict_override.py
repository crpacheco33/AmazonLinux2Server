"""Overrides Python `dict`s with custom behavior."""


class Keypath(dict):
    """Introduces property-like keys on `dict`."""

    def __init__(self, data):
        """Initializes instance of Keypath
        
        Args
            data: `dict` to convert to property-like instance

        Returns:
            None
        """
        super(Keypath, self).__init__(data)

        for key in self:
            value = self[key]
            if isinstance(value, list):
                for index, item in enumerate(value):
                    if isinstance(item, dict):
                        value[index] = Keypath(item)

            elif isinstance(value, dict):
                self[key] = Keypath(value)

    def to_dict(self):
        """Transforms instance to `dict`.
        
        Returns:
            `dict` equivalent of `Keypath` instance
        """
        return dict(self)

    def __getattr__(self, key):
        return self[key]
