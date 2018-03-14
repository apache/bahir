try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='mqtt',
    version='2.3.0-SNAPSHOT',
    py_modules=['mqtt']
)

