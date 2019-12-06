from skyhook import SkyhookDM
sk = SkyhookDM()
sk.connect('localhost')
urls = ['https://github.com/uccross/skyhookdm-pythonclient/blob/master/nano_aod.root?raw=true']
sk.writeDataset(urls,'nanoexample')
sk.getDataset('nanoexample')