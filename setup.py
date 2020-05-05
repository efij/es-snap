from setuptools import setup



# some more details
CLASSIFIERS = [
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
]

# calling the setup function
setup(name='elastic',
      version='1.0.0',
      packages=['elastic'],
      package_data={'elastic': ['*',]},
      )
