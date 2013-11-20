import os
import sys

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open('README', 'rb').read()
CHANGES = open('CHANGES', 'rb').read()

requires = [
    'beaker',
    'redis',
    ]

setup(name='beaker_backend_redis_ns',
      version='0.1',
      description='Namespaced Redis backend for beaker',
      long_description=README + '\n\n' +  CHANGES,
      classifiers=[
        "Programming Language :: Python",
        "Framework :: Pylons",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
        ],
      author='Brendan Zerr',
      author_email='bzerr@brainwire.ca',
      url='http://brainwire.ca',
      keywords='web wsgi bfg pylons pyramid',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      install_requires = requires,
      entry_points = """
      [beaker.backends]
      redis_ns = beaker_backend_redis_ns.redis_:RedisManager
      """,
      )

