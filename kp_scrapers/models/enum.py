"""Module for custom readonly Enum class.

The Enum object provides support for robust, readonly properties and iteration.

Usage
~~~~~

    .. code-block:: Python

        earth = Enum(continents=7, age='4.5 billion years')

        # iterate across all enums
        list(earth)
        dict(earth)

        # view enum values
        print(earth.age)

        # try and add new enums (this will throw an error as expected)
        earth.oceans = 5

        # modify an existing enum value (this will throw an error as expected)
        earth.age = '6000 years'

"""
import sys


class Enum:
    def __init__(self, **enums):
        """Instantitate Enum object with provided key-value pairs, and set attributes to readonly.

        Args:
            enums (Dict[str, str]): key-value pairs of enums and their corresponding values
        """
        for key, value in enums.items():
            self.__setattr__(key, value)

    def __setattr__(self, attr, value):
        """Override default attribute setting method and prevent modification of object attributes.

        Since this class is meant to represent an enum, we don't want it to be possible
        to modify enum values or append new enums when it has been instantiated.

        Args:
            attr (str): attribute name to assign value to
            value (str): value to assign to attribute

        """
        # restrict existing enums from being modified
        if attr in vars(self) or attr in self.__dict__:
            raise AttributeError('Cannot modify existing enum values')
        # restrict new enums from being appended to instantiated object
        if sys._getframe().f_back.f_code.co_name != '__init__':
            raise AttributeError('Cannot append new enums')

        super().__setattr__(attr, value)

    def __iter__(self):
        """Allow iteration over an instantiated Enum object.

        Yields:
            Tuple[str, str]: tuple of enum (key, value)

        """
        for attr, value in vars(self).items():
            if not attr.startswith('_'):
                yield attr, value
