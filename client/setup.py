from distutils.core import setup
setup(
  name = 'skyhookdmclient',         # How you named your package folder (MyLib)
  packages = ['skyhookdmclient'],   # Chose the same as "name"
  version = '0.0.12',      # Start with a small number and increase it with every change you make
  license='MIT',        # Chose a license from here: https://help.github.com/articles/licensing-a-repository
  description = 'Python client of SkyhookDM for Ceph',   # Give a short description about your library
  author = 'Xiaowei Chu',                   # Type in your name
  author_email = 'xweichu@hotmail.com',      # Type in your E-Mail
  url = 'https://github.com/uccross/skyhookdm-pythonclient',   # Provide either the link to your github or to your website
  # download_url = 'https://github.com/uccross/skyhookdm-pythonclient/archive/v0.3.4.tar.gz',    # I explain this later on
  keywords = ['SkyhookDM', 'Skyhook', 'Skyhook-pythonclient'],   # Keywords that define your package best
  install_requires=[
          'pyarrow',
          'zict==1.0.0',
          'msgpack==0.6.2',
          'uproot',
          'dask[complete]'
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
    'Intended Audience :: Developers',      # Define that your audience are developers
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',   # Again, pick a license
    'Programming Language :: Python :: 3.6',      # Specify which pyhton versions that you want to support
  ],
)
