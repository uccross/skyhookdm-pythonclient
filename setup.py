from distutils.core import setup
setup(
  name = 'skyhookdmpy',         # How you named your package folder (MyLib)
  packages = ['skyhookdmpy'],   # Chose the same as "name"
  version = '0.2.9',      # Start with a small number and increase it with every change you make
  license='MIT',        # Chose a license from here: https://help.github.com/articles/licensing-a-repository
  description = 'Python client of Skyhook for Ceph',   # Give a short description about your library
  author = 'Xiaowei Chu',                   # Type in your name
  author_email = 'xweichu@hotmail.com',      # Type in your E-Mail
  url = 'https://github.com/uccross/skyhookdm-pythonclient',   # Provide either the link to your github or to your website
  download_url = 'https://github.com/uccross/skyhookdm-pythonclient/archive/v0.2.7.tar.gz',    # I explain this later on
  keywords = ['SkyhookDM', 'Skyhook', 'Skyhook-pythonclient'],   # Keywords that define your package best
  install_requires=[            # I get to this in a second
          'pyarrow',
          'msgpack==0.6.2'
          'uproot',
          'bokeh',
          'wget',
          'backports.lzma',
          'dask[complete]',
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
    'Intended Audience :: Developers',      # Define that your audience are developers
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',   # Again, pick a license
    'Programming Language :: Python :: 2.7',      #Specify which pyhton versions that you want to support
  ],
)